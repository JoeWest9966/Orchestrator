//! exec —— 派发序列（spec §5.3 派发序列 / §5.4 回收接线）。
//!
//! 本模块把 decide 定下的 `Decision::Dispatch`（`crate::tick`）展开成契约调用序列，并且只为
//! 这一张票做两件事：**派发**与**失败回滚**。交还一张已完成的票（spec §5.4「`merge` ⇒
//! 移出活跃集」）也在这里——[`release_ticket`] 是那条路上**唯一**的动作。
//!
//! ## 契约调用序列（spec §5.3）
//!
//! ```text
//! 1. GET  /v1/workspace/mappings       只读：拼出建 worktree 的请求（推不出就不抢票）
//! 2. POST /v1/tracker/claims           幂等 claim（我已持有 ⇒ 200 + claimed=true）
//! 3. POST /v1/tracker/claims           revalidation：同一 assignee 重 claim 恒 claimed=true
//! 4. POST /v1/workspace/worktrees      委托 workspace 服务建 worktree
//! 5. POST /v1/agent-runner/dispatches  prompt_template 展开 {{ticket_id}}
//! ```
//!
//! 票面写的是 `claim → revalidation → establish worktree → dispatch` 四步；上面第 1 步只是四步的
//! **只读前置**——`WorktreeEstablishRequest` 的 `spec_id` / `integration_branch` 只能从 mappings 读
//! （见下 `[UNSOURCED]`），而推不出时不该先把票抢到手（抢到却派不出去只会占着不动）。
//!
//! `prompt_template` 的占位符：票面只命名 `{{ticket_id}}`（spec §5.3），所以只替换它；
//! 别的占位符是否存在是 `[UNSOURCED]`，未识别的 `{{...}}` 原样留在 prompt 里，不猜、不吞。
//! `DispatchRequest.profile`（内容物装配标识）同样**没有任何来源** ⇒ 恒 `None`。
//!
//! ## `[UNSOURCED]`：建 worktree 的三条路径
//!
//! `WorktreeEstablishRequest` 有六个字段，其中 `spec_id` / `integration_branch` 能从
//! `GET /v1/workspace/mappings` 读出来（复用本票的 worktree 映射，或唯一那条
//! `(spec_id, integration_branch)` 映射），而 `repo_path` / `worktree_path` / `worktree_branch`
//! **契约面推不出**：编排器的 env 闭集（票 #5：`FARM_HOST` / `FARM_TOKEN` / `FARM_ADMIN_TOKEN` /
//! `FARM_ASSIGNEE` / `TICK_INTERVAL_MS`）里没有它们，契约里也没有「worktree 根目录 / 分支命名」
//! 这种约定字段。故它们由调用方经 [`WorktreeLayout`] 供给——本模块**不发明**变量名，也不发明
//! 路径与分支的命名约定。
//!
//! ## 闭集分流（10 码，`farm_sdk::ErrorCode` 是唯一来源）
//!
//! | 码 | 处置 | 交还 | 冷却 | 告警 |
//! | --- | --- | --- | --- | --- |
//! | `service_unavailable` | 重试（唯一 `is_retryable()` 为真的码） | 否 | 否 | 否 |
//! | `conflict` | 放弃：票不在我名下 | 否 | 是 | 否 |
//! | `not_found` | 放弃：票不在视图里 | 否 | 是 | 否 |
//! | `invalid_request` | 放弃 + 交还：只可能是我们自己编错了请求 | 是 | 是 | 是 |
//! | `unauthorized` / `forbidden` / `contract_version_unsupported` / `method_not_allowed` / `not_implemented` / `internal` | 硬失败：本 tick 余下的序列不再发契约调用 | 尽力 | 是 | 是 |
//!
//! 分流判据一律取 [`ErrorCode`] 的枚举与 `is_retryable()`——**零硬编码码字符串**（码字符串只
//! 出现在告警字段里，且取自 `ErrorCode::as_str()`）。与 spec §5.3 那句话「非
//! `service_unavailable` 失败 → release + 记冷却」相比只有一处收紧：`conflict` 与 `not_found`
//! 不发 release——这两个码本身就是契约在说「票不在你名下」/「票不在视图里」，交还只会再吃同一个码。
//!
//! ## 回收接线（spec §5.4）
//!
//! 「事件 → 世界」那一半（`blocked` / `needs_attention` 只登记、`stall_kill` / `interrupted`
//! 按退避计冷却、`merge` 移出活跃集）是 `crate::recon` 的折叠，本模块**不重复**它（单一源：
//! 事件的解释只有一处）。这里接的是折叠之后的那一半：
//!
//! * 回收后仍在我名下、未完成、冷却已过 ⇒ decide 发 `Decision::Dispatch` ⇒ [`dispatch_ticket`]；
//! * 回收后已完成（`merge` / 完成信号）⇒ decide 发 `Decision::Release` ⇒ [`release_ticket`]：
//!   一条 release 契约调用，**别的什么都不做**。关票 / 开 PR / 评论 / 打标签是宿主的机械投影
//!   （宿主按 workflow 配置自己执行），本模块连配置里那组开关都不读；
//! * `blocked` / `needs_attention` 不结束 attempt，世界里的活跃 attempt 还在 ⇒ 决策面表现为
//!   「票还在活跃集里」⇒ 零动作、零冷却：登记是 `recon` 的活。
//!
//! 交还本身也没有补偿动作：`ReleaseOutcome::NotClaimed`（无人持有）是幂等空操作，不是错误。

use farm_sdk::services::agent_runner::DispatchRequest;
use farm_sdk::services::tracker::{ClaimResponse, ReleaseOutcome};
use farm_sdk::services::workspace::{
    MappingListResponse, WorkspaceMapping, WorktreeEstablishRequest,
};
use farm_sdk::workflow::WorkflowConfig;
use farm_sdk::ErrorCode;

use crate::contract::client::ContractClient;

/// `prompt_template` 里唯一被命名的占位符（spec §5.3）。
const TICKET_ID_PLACEHOLDER: &str = "{{ticket_id}}";

// ---------------------------------------------------------------------------
// 输入：契约推不出的那一小块
// ---------------------------------------------------------------------------

/// 建 ticket worktree 需要的、**契约面推不出**的三个路径字段。
///
/// 来源是 `[UNSOURCED]`（见模块头）：本模块不猜环境变量名，也不猜路径 / 分支的命名约定，
/// 只把这个输入原样收在参数里，由调用方（tick 循环，票 #5）供给。
///
/// * `repo_path`：源仓库路径（`git worktree add` 的目标）——**始终**用到（mappings 视图里没有它）。
/// * `worktree_path` / `worktree_branch`：只在**本票还没有 worktree 映射**时用于新建；已有映射时
///   按映射里的 path / branch 复用（宿主侧的 establish 本身也是幂等的，所以复用不需要我们判断
///   「目录还健康吗」——那是宿主的活）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorktreeLayout {
    pub repo_path: String,
    pub worktree_path: String,
    pub worktree_branch: String,
}

// ---------------------------------------------------------------------------
// 闭集分流
// ---------------------------------------------------------------------------

/// 一个契约错误在派发序列里的处置（本模块自己的闭集，不是 wire 类型）。
///
/// 变体就是**动作集**本身：交不交还、记不记冷却、告不告警、本 tick 还继续不继续。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// 宿主暂时不可用（唯一 `is_retryable()` 为真的码）：不放弃本票、不交还、不冷却，退避后重试。
    Retry,
    /// 放弃本票，**不交还**：票不在我名下（`conflict`）或不在视图里（`not_found`）——交还无从谈起。
    /// 冷却照记（同一张票在下一次冷却到期前不碰）。
    AbandonedNotMine,
    /// 放弃本票 + **交还**（我方 bug：请求不合法只可能是我们自己编错了）：告警一次，记冷却。
    AbandonedAndReleased,
    /// 硬失败：凭据 / 契约版本 / 路由方法 / 宿主实现面不可自愈。告警一次，交还尽力而为，记冷却；
    /// 本 tick 余下的序列不该再发契约调用（这一处置是给 tick 循环的信号，本模块只报出来）。
    Fatal,
}

/// 十码穷举分流：每个码都有自己的分支（`match` 里逐码成臂，没有 `_`——新增码是契约变更，
/// 编译器会在这里点出来）。
///
/// 可重试性由 [`ErrorCode::is_retryable`] 单一源判定，不在这里硬编码哪个码可重试；
/// 不可重试的码再逐码成臂。判据只取枚举本身，不取 HTTP 状态码、不取字符串。
pub fn route(code: ErrorCode) -> Disposition {
    if code.is_retryable() {
        return Disposition::Retry;
    }
    match code {
        ErrorCode::Unauthorized => Disposition::Fatal,
        ErrorCode::Forbidden => Disposition::Fatal,
        ErrorCode::ContractVersionUnsupported => Disposition::Fatal,
        ErrorCode::InvalidRequest => Disposition::AbandonedAndReleased,
        ErrorCode::NotFound => Disposition::AbandonedNotMine,
        ErrorCode::MethodNotAllowed => Disposition::Fatal,
        ErrorCode::Conflict => Disposition::AbandonedNotMine,
        ErrorCode::NotImplemented => Disposition::Fatal,
        ErrorCode::ServiceUnavailable => unreachable!("service_unavailable is retryable"),
        ErrorCode::Internal => Disposition::Fatal,
    }
}

// ---------------------------------------------------------------------------
// 输出：结局与回滚
// ---------------------------------------------------------------------------

/// 冷却登记：一张票在某个延迟内不再被碰。
///
/// 延迟取自 effective `RetryBackoff.base_delay_ms`——本次失败是本 attempt 的**第 1 次**冷却触发
/// （宿主不会为一次没起来的 dispatch 发事件，事件流里的档位计数器还是 0），而第 1 档延迟就是
/// `base_delay_ms`；后续档位由事件流（`crate::recon`）计，本模块不自己攒计数器（ADR-0002：零持久状态）。
///
/// 截止时刻由调用方折进世界的 `backoff_until`（`crate::recon` 从事件时间戳算截止时刻走的是同一条路）：
/// 时间戳算术只有一处，本模块不碰钟面。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cooldown {
    pub ticket_id: String,
    pub delay_ms: u64,
}

/// 失败回滚记录（spec §5.3）：这一票在放弃时做的两步——尽力交还 + 记冷却。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rollback {
    /// 交还的结局。`None` = 该码的分流决定不发交还（票不在我名下）；
    /// `Some(Ok(NotClaimed))` = 无人持有，幂等空操作（不是错误）；`Some(Err(code))` = 连交还也失败——
    /// 记录在案，不掩盖、也不再递归回滚。
    pub release: Option<Result<ReleaseOutcome, ErrorCode>>,
    /// 冷却登记。
    pub cooldown: Cooldown,
}

/// 一张票的派发序列结局（闭集）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// 派发成功：宿主已起 attempt（`DispatchResponse.attempt_id`）。
    Dispatched { attempt_id: String },
    /// 本 tick 内可重试（唯一 `is_retryable()` 的码）：未交还、未冷却——下一次来的是同一张票。
    Retried { code: ErrorCode },
    /// 放弃本票（票不在我名下 / 我方 bug），已按分流做过回滚。
    Abandoned { code: ErrorCode, rollback: Rollback },
    /// 硬失败（不可自愈），已按分流做过回滚。
    HardFailure { code: ErrorCode, rollback: Rollback },
    /// 契约读数推不出建 worktree 需要的 `(spec_id, integration_branch)`（见模块头 `[UNSOURCED]`）：
    /// 不猜、不抢票、零契约调用（除那条只读快照）、零冷却，只告警一次。
    LayoutUnderivable,
}

/// 交还一张票（spec §5.4「完成信号 ⇒ 移出活跃集」）的结局（闭集）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReleaseStep {
    /// 交还完成：真的交还了，或本来就无人持有（幂等空操作）。
    Released { outcome: ReleaseOutcome },
    /// 交还暂时失败（唯一可重试码）：本 tick 退避后可再试。
    Retried { code: ErrorCode },
    /// 交还硬失败（其余码）：告警 + 记冷却，本 tick 不再试。
    Failed { code: ErrorCode, cooldown: Cooldown },
}

// ---------------------------------------------------------------------------
// 派发序列
// ---------------------------------------------------------------------------

/// 跑一张票的派发序列（spec §5.3）：claim → revalidation → establish worktree → dispatch。
///
/// 契约调用的顺序与每一步的请求形状见模块头；失败时按 [`route`] 分流并回滚。
pub async fn dispatch_ticket(
    client: &ContractClient,
    assignee: &str,
    ticket_id: &str,
    workflow: &WorkflowConfig,
    layout: &WorktreeLayout,
) -> Outcome {
    // 1. 只读前置：拼出建 worktree 的请求。推不出就不抢票。
    let request = match client.mappings().await {
        Ok(mappings) => worktree_request(&mappings, ticket_id, layout),
        Err(error) => return roll_back(client, assignee, ticket_id, workflow, error.code).await,
    };
    let Some(request) = request else {
        tracing::warn!(
            alert = "worktree_layout_underivable",
            ticket = ticket_id,
            "mappings 里既没有本票的 worktree，也没有唯一一条 (spec_id, integration_branch)：不猜，跳过本票"
        );
        return Outcome::LayoutUnderivable;
    };

    // 2. claim；3. revalidation（同一个幂等调用两遍——票面把 revalidation 单独列了一步）。
    if let Some(outcome) = claim_step(client, assignee, ticket_id, workflow).await {
        return outcome;
    }
    if let Some(outcome) = claim_step(client, assignee, ticket_id, workflow).await {
        return outcome;
    }

    // 4. 建 worktree（委托 workspace 服务；宿主侧 write-ahead + 幂等）。
    if let Err(error) = client.establish_worktree(&request).await {
        return roll_back(client, assignee, ticket_id, workflow, error.code).await;
    }

    // 5. 派发。
    let request = DispatchRequest {
        issue_id: ticket_id.to_string(),
        // 内容物装配标识没有来源（票面与契约都没给）⇒ 不发明，恒 `None`。
        profile: None,
        prompt: Some(expand_ticket_placeholder(
            &workflow.prompt_template,
            ticket_id,
        )),
    };
    match client.dispatch(&request).await {
        Ok(response) => Outcome::Dispatched {
            attempt_id: response.attempt_id,
        },
        Err(error) => roll_back(client, assignee, ticket_id, workflow, error.code).await,
    }
}

/// 交还一张票（spec §5.4：完成信号 ⇒ 移出活跃集）。本模块在这条路上只打这一条契约路由。
pub async fn release_ticket(
    client: &ContractClient,
    assignee: &str,
    ticket_id: &str,
    workflow: &WorkflowConfig,
) -> ReleaseStep {
    match client.release(ticket_id, assignee).await {
        Ok(response) => ReleaseStep::Released {
            outcome: response.outcome,
        },
        Err(error) => match route(error.code) {
            Disposition::Retry => ReleaseStep::Retried { code: error.code },
            _ => {
                tracing::warn!(
                    alert = "release_failed",
                    code = error.code.as_str(),
                    ticket = ticket_id,
                    "交还失败：本 tick 不再试，记冷却"
                );
                ReleaseStep::Failed {
                    code: error.code,
                    cooldown: cooldown_for(workflow, ticket_id),
                }
            }
        },
    }
}

// ---------------------------------------------------------------------------
// 内部：分流后的回滚
// ---------------------------------------------------------------------------

/// 序列的 claim 步（两次调用同形：claim + revalidation）。
///
/// `None` = 票确实在我名下，继续往下走；`Some(outcome)` = 序列到此为止。
async fn claim_step(
    client: &ContractClient,
    assignee: &str,
    ticket_id: &str,
    workflow: &WorkflowConfig,
) -> Option<Outcome> {
    match client.claim(ticket_id, assignee).await {
        // `claimed == false` 的解释（`interpret_claim`）只有一处：契约说这就是「已被他人持有」。
        Ok(response) => match interpret_claim(&response) {
            Ok(()) => None,
            Err(code) => Some(roll_back(client, assignee, ticket_id, workflow, code).await),
        },
        Err(error) => Some(roll_back(client, assignee, ticket_id, workflow, error.code).await),
    }
}

/// 失败 → 分流 → 回滚（spec §5.3：非 `service_unavailable` 失败 ⇒ release + 记冷却）。
async fn roll_back(
    client: &ContractClient,
    assignee: &str,
    ticket_id: &str,
    workflow: &WorkflowConfig,
    code: ErrorCode,
) -> Outcome {
    let cooldown = cooldown_for(workflow, ticket_id);
    match route(code) {
        // 重试语义：不交还、不冷却——退避后再来的是同一张票。
        Disposition::Retry => Outcome::Retried { code },
        Disposition::AbandonedNotMine => Outcome::Abandoned {
            code,
            rollback: Rollback {
                release: None,
                cooldown,
            },
        },
        Disposition::AbandonedAndReleased => {
            let release = best_effort_release(client, assignee, ticket_id).await;
            tracing::warn!(
                alert = "dispatch_request_invalid",
                code = code.as_str(),
                ticket = ticket_id,
                "契约拒绝了我们的请求（我方 bug）：放弃本票并交还"
            );
            Outcome::Abandoned {
                code,
                rollback: Rollback {
                    release: Some(release),
                    cooldown,
                },
            }
        }
        Disposition::Fatal => {
            let release = best_effort_release(client, assignee, ticket_id).await;
            tracing::warn!(
                alert = "dispatch_sequence_fatal",
                code = code.as_str(),
                ticket = ticket_id,
                "不可自愈的契约失败：放弃本票、尽力交还，本 tick 不再发契约调用"
            );
            Outcome::HardFailure {
                code,
                rollback: Rollback {
                    release: Some(release),
                    cooldown,
                },
            }
        }
    }
}

/// 尽力交还（幂等）：`NotClaimed` 是空操作不是错误；失败只把闭集码记进回滚，不再递归回滚。
async fn best_effort_release(
    client: &ContractClient,
    assignee: &str,
    ticket_id: &str,
) -> Result<ReleaseOutcome, ErrorCode> {
    client
        .release(ticket_id, assignee)
        .await
        .map(|response| response.outcome)
        .map_err(|error| error.code)
}

/// 冷却登记（延迟取自 effective `RetryBackoff`；见 [`Cooldown`]）。
fn cooldown_for(workflow: &WorkflowConfig, ticket_id: &str) -> Cooldown {
    Cooldown {
        ticket_id: ticket_id.to_string(),
        delay_ms: workflow.retry_backoff.base_delay_ms,
    }
}

/// `{{ticket_id}}` 占位符展开：只替换票面命名的这一个占位符，未识别的 `{{...}}` 原样留着。
///
/// 两处调用共用同一个展开器：`prompt_template`（本模块）与 worktree 路径 / 分支模板
/// （`crate::main_loop` 的 `FARM_WORKTREE_PATH` / `FARM_WORKTREE_BRANCH`——那两条是票面
/// env 闭集之外的扩展，见 `main_loop` 模块头的 `[UNSOURCED]`）。它们借用的是契约
/// `WorkflowConfig::prompt_template` 已经文档化的**同一个**占位符机制，不是新发明。
pub fn expand_ticket_placeholder(template: &str, ticket_id: &str) -> String {
    template.replace(TICKET_ID_PLACEHOLDER, ticket_id)
}

/// 拼一张票的建 worktree 请求：`spec_id` / `integration_branch` 取自 mappings，三条路径里
/// `worktree_path` / `worktree_branch` 优先复用本票已有的 worktree 映射（`repo_path` 只能来自
/// [`WorktreeLayout`]——映射视图里没有它）。
///
/// 推不出就返回 `None`：mappings 里既没有本票的 worktree，也没有**唯一**一条
/// `(spec_id, integration_branch)` 可当作本编排器的目标（空列表、或多条并存都算推不出）。
/// 多条并存时没有任何契约字段能挑出「本编排器的那条」——不猜，交给人的裁决。
/// `ClaimResponse` 的解释：拿到 ⇒ `Ok(())`；没拿到 ⇒ `Err(ErrorCode::Conflict)`。
///
/// `claimed == false` 在契约里的语义就是「已被他人持有」（`ClaimResponse` 的注释：调用方本会
/// 收到 `conflict`）。这一个映射只有一处——本模块的 claim 步与主循环的 `Decision::Claim` 臂
/// 都走它，不再各写一遍。
pub fn interpret_claim(response: &ClaimResponse) -> Result<(), ErrorCode> {
    if response.claimed {
        Ok(())
    } else {
        Err(ErrorCode::Conflict)
    }
}

/// 从 workspace 映射里挑出「本编排器的那一条」。
///
/// 判据（只有一个，两处调用方共用）：**先按票挑**——哪条映射的 worktrees 里命中 `owns` 就取它；
/// 一条都没命中时，**只有恰好一条映射**才兜底用它；多条并存而无一命中 ⇒ `None`——没有任何契约
/// 字段能指出「本编排器的那条」，不猜。
///
/// 主循环用它挑 `repo_id`（生效配置的键），本模块的 [`worktree_request`] 用它挑
/// `(spec_id, integration_branch)`：同一条判据，不再有两份实现。
pub fn select_mapping(
    mappings: &MappingListResponse,
    owns: impl Fn(&str) -> bool,
) -> Option<&WorkspaceMapping> {
    let hits: Vec<&WorkspaceMapping> = mappings
        .mappings
        .iter()
        .filter(|mapping| {
            mapping
                .worktrees
                .iter()
                .any(|worktree| owns(worktree.ticket_id.as_str()))
        })
        .collect();
    match hits.as_slice() {
        // 确定性：同一份读数下挑出的恒是同一条（映射顺序不保证，取 spec_id 字典序最小）。
        [_, ..] => hits.into_iter().min_by(|a, b| a.spec_id.cmp(&b.spec_id)),
        [] => match mappings.mappings.as_slice() {
            [only] => Some(only),
            _ => None,
        },
    }
}

fn worktree_request(
    mappings: &MappingListResponse,
    ticket_id: &str,
    layout: &WorktreeLayout,
) -> Option<WorktreeEstablishRequest> {
    let mapping = select_mapping(mappings, |ticket| ticket == ticket_id)?;
    let spec_id = mapping.spec_id.clone();
    let integration_branch = mapping.integration_branch.clone();
    // 本票已有 worktree ⇒ 路径与分支复用它（契约读数，不发明命名）；还没有（选中它靠的是
    // 「唯一一条映射」兜底）⇒ 用调用方经 [`WorktreeLayout`] 给的模板值。
    let existing = mapping
        .worktrees
        .iter()
        .find(|worktree| worktree.ticket_id == ticket_id);
    let worktree_path = existing
        .map(|worktree| worktree.path.clone())
        .unwrap_or_else(|| layout.worktree_path.clone());
    let worktree_branch = existing
        .map(|worktree| worktree.branch.clone())
        .unwrap_or_else(|| layout.worktree_branch.clone());
    Some(WorktreeEstablishRequest {
        spec_id,
        integration_branch,
        ticket_id: ticket_id.to_string(),
        repo_path: layout.repo_path.clone(),
        worktree_path,
        worktree_branch,
        // 建分支的起点：票面与契约都没有它的来源 ⇒ 不发明，让宿主按 HEAD 建。
        start_point: None,
    })
}

#[cfg(test)]
mod tests;

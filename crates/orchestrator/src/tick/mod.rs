//! tick —— tick 循环的决策段：reconcile → **decide（纯函数，零 IO）** → execute（ADR-0001）。
//!
//! 本模块只把「本 tick 的世界」映射成「本 tick 要打的契约调用序列」。它不是执行器
//! （那是 ticket #4），也不碰钟面：`now` 由 tick 循环注入——世界里的全部时间戳
//! （`backoff_until`、`event_watermark`）都是宿主事件时间戳形状（RFC3339、UTC、秒级，
//! 见 `recon` 模块头），字典序即时间序，所以时间判断只需要比较字符串。
//!
//! 决策面的输入是四项只读借用（tick 序列 = spec §2.2）：
//!
//! | 输入 | 来源 |
//! | --- | --- |
//! | [`WorldModel`] | 对账重建（ticket #2，`recon`） |
//! | [`IssueListResponse`] | 本 tick 的 issues 快照（frontier 要看 `state` / `labels` / `blocked_by` / `assignee`） |
//! | [`EffectiveConfig`] | 本 tick 的生效配置（ADR-0004） |
//! | `now` | 本 tick 的钟面，由 tick 循环注入 |
//!
//! ticket #3 的字面签名只写了 `(WorldModel, EffectiveConfig)`；issues 快照是**必需的**第四项
//! 输入——`WorldModel` 是 ticket #2 的**五字段闭集**，里面没有 `IssueView`，而 frontier 必须
//! 看 `IssueView` 的字段。这一点已与决策方确认。`now` 的确切来源（宿主事件时间戳 vs 编排器
//! 本地钟）在票面与 Farm 源里都是 `[UNSOURCED]`：本模块只**接收**它，不取它。

use std::collections::BTreeSet;

use farm_sdk::services::tracker::{IssueListResponse, IssueState};
use farm_sdk::workflow::{EffectiveSource, WorkflowConfig, WorkflowConfigStatus};

use crate::recon::WorldModel;

/// 本 tick 的生效配置：管理面读数（`WorkflowConfigStatus`）里调度需要的两个字段。
///
/// ADR-0004 的三态里，[`decide`] 只看 `effective`：`None` ⇒ 无决策（idle）。「告警」与
/// LKG 的「持续标注」是 tick 循环（ticket #5）的活——它们是 IO，而本模块零 IO；
/// `source` 由 [`decide`] 原样带过、不被分支，正是为了让 #5 有得可标注。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectiveConfig {
    /// `WorkflowConfigStatus.effective`；`None` = 尚无生效配置（首次即非法，拒绝编排）。
    pub effective: Option<WorkflowConfig>,
    /// `WorkflowConfigStatus.source`；生效配置来自 `fresh` 还是 `last_known_good`。
    pub source: Option<EffectiveSource>,
}

impl EffectiveConfig {
    /// 从管理面读数派生（`GET /admin/workflow/config/{repo_id}` 的响应）。
    pub fn from_status(status: &WorkflowConfigStatus) -> Self {
        Self {
            effective: status.effective.clone(),
            source: status.source,
        }
    }
}

/// 一个 tick 的决策（闭集）：抢票 / 派发 / 交还。tick 无话可说时决策表为空。
///
/// 三种决策各自对应契约面上的一段调用，展开成调用序列是执行器的活（ticket #4）：
///
/// * [`Decision::Claim`] → `POST /v1/tracker/claims`
/// * [`Decision::Dispatch`] → claim 幂等 revalidation → `POST /v1/workspace/worktrees` → `POST /v1/agent-runner/dispatches`
/// * [`Decision::Release`] → `POST /v1/tracker/releases`
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// 抢票：把 frontier 里的票拿到自己名下（排他所有权）。
    Claim { ticket_id: String },
    /// 派发：跑一张已在我名下、当前没有活跃 attempt 的票。
    Dispatch { ticket_id: String },
    /// 交还：把已经完成的票的排他所有权交还（票面 §5.4「`merge` ⇒ 移出活跃集」）。
    Release { ticket_id: String },
}

/// 决策面的类型层证明：`decide` 是普通同步函数——输入只有四个只读借用，输出是决策表。
///
/// 它不是可等待体、不返回 `Result`（没有 IO 错误面）、输入里没有任何客户端 / 句柄 / 钟面，
/// 也没有 `self`（无状态）。任何要做 IO 的实现都改不动这个类型。
const _: fn(&WorldModel, &IssueListResponse, &EffectiveConfig, &str) -> Vec<Decision> = decide;

/// 本 tick 的决策表。**纯函数**：同 `(world, issues, config, now)` 恒出同决策，且输出与
/// 输入集合里的元素顺序无关（前沿按票 id 升序、三段定序，见下）。
///
/// **frontier**（spec §5.1，按票 id 升序）：同时满足
///
/// * `IssueView.state == open`；
/// * `labels ∩ config.dispatch_labels ≠ ∅`；
/// * 每张 `blocked_by` 的票都已**完成**；
/// * 票不在任何人名下（`assignee == None`）——票面把「排除已被任何人 claim 的票」单独写了
///   出来。宿主的 claim 会把 `open` 原子推到 `claimed`，所以这条是 `state == open` 的隐含
///   不变量；这里仍然显式检查 `assignee`，不靠宿主实现里的隐含不变量。
///
/// **完成** = `IssueView.state == done` ∨ 票 id ∈ `world.merge_done`。`merge_done` 是 ticket #2
/// 的完成信号（含事件窗口被截断时回退快照的退化路径），而投影开关
/// （`ProjectionSwitches.close_issue`，默认 **false**）不保证 merge 之后票一定变 `done`——
/// 只认快照状态会让依赖票永久堵死。
///
/// **dispatchable**（spec §5.2）：并发配额与冷却——
///
/// * 余量 = `config.max_concurrency - world.active_attempts.len()`。本 tick 定下的每个
///   [`Decision::Claim`] 与 [`Decision::Dispatch`] 都从同一份余量里扣：只按 `active` 判会让
///   一个 tick 把整个 frontier 抢光，下个 tick 再一次性派发，超出配额。
/// * 冷却中 = `world.backoff_until[ticket_id] > now`（截止时刻还没到）。冷却中的票本 tick
///   不碰：抢票也不抢——抢来只会占着不动，而等待的是同一个冷却。
///
/// **决策表的三段与定序**（`交还 → 派发 → 抢票`，段内按票 id 升序）：
///
/// 1. [`Decision::Release`]：我名下 ∧ 无活跃 attempt ∧ 已完成。票面没说 decide 何时发 release
///    （见下），这是决策方的取法：完成信号的排他所有权没有用途，交还幂等且可测。
///    活跃 attempt 还在跑的票**绝不**交还——那是把正在干活的票扔给下一个抢的人。
/// 2. [`Decision::Dispatch`]：我名下 ∧ 无活跃 attempt ∧ 未完成 ∧ 不在冷却中。崩溃重启后的
///    续跑走的正是这条：票面 §5.4 把尝试的终态（`interrupted` 等）算成冷却，冷却过去之后
///    我仍持有 claim，于是重新派发。
/// 3. [`Decision::Claim`]：frontier 里剩下的票（不在冷却中）。
///
/// 派发排在抢票前面：我已持有的票优先占用余量，余量才用来接新活。
///
/// 票面未定、由本票取法的两处（判据可复核，不是 spec 要求）：`Release` 的产生条件；
/// 抢票同样受并发配额与冷却约束。
pub fn decide(
    world: &WorldModel,
    issues: &IssueListResponse,
    config: &EffectiveConfig,
    now: &str,
) -> Vec<Decision> {
    // ADR-0004：尚无生效配置 ⇒ 不编排（idle + 告警）。告警是 #5 的活（IO），本模块只负责
    // 「无决策」。`source == last_known_good` 照常编排，所以 `source` 不参与分支。
    let Some(workflow) = &config.effective else {
        return Vec::new();
    };

    let finished = finished_tickets(world, issues);
    let frontier = frontier(issues, &finished, workflow);

    let mut budget = workflow
        .max_concurrency
        .saturating_sub(u32::try_from(world.active_attempts.len()).unwrap_or(u32::MAX));
    let mut decisions = Vec::new();

    // 1. 交还。
    for ticket_id in &world.claimed_by_me {
        if world.active_attempts.contains_key(ticket_id) || !finished.contains(ticket_id.as_str()) {
            continue;
        }
        decisions.push(Decision::Release {
            ticket_id: ticket_id.clone(),
        });
    }

    // 2. 派发我已持有的票。
    for ticket_id in &world.claimed_by_me {
        if budget == 0 {
            break;
        }
        if world.active_attempts.contains_key(ticket_id)
            || finished.contains(ticket_id.as_str())
            || in_cooldown(world, ticket_id, now)
        {
            continue;
        }
        decisions.push(Decision::Dispatch {
            ticket_id: ticket_id.clone(),
        });
        budget -= 1;
    }

    // 3. 抢 frontier 里剩下的票。
    for ticket_id in frontier {
        if budget == 0 {
            break;
        }
        if in_cooldown(world, ticket_id, now) {
            continue;
        }
        decisions.push(Decision::Claim {
            ticket_id: ticket_id.to_string(),
        });
        budget -= 1;
    }

    decisions
}

/// frontier（spec §5.1）：就绪、无人持有、还没被谁抢走的票，按票 id 升序。
///
/// `blocked_by` 上引用了快照里查不到的票时**不算完成**（推不出就不猜，也不把它当无依赖的
/// 节点）——所以自环与环状 `blocked_by` 里的票永远算不出就绪，判定自然终止，不需要在这里
/// 再做一遍环检测（检测与告警是 `recon` 的活，ticket #2）。
fn frontier<'a>(
    issues: &'a IssueListResponse,
    finished: &BTreeSet<&str>,
    workflow: &WorkflowConfig,
) -> Vec<&'a str> {
    let candidates: BTreeSet<&str> = issues
        .issues
        .iter()
        .filter(|issue| {
            issue.state == IssueState::Open
                && issue.assignee.is_none()
                && issue
                    .labels
                    .iter()
                    .any(|label| workflow.dispatch_labels.contains(label))
        })
        .filter(|issue| {
            issue
                .blocked_by
                .iter()
                .all(|blocker| finished.contains(blocker.as_str()))
        })
        .map(|issue| issue.issue_id.as_str())
        .collect();
    candidates.into_iter().collect()
}

/// 已完成的票：快照说 `done`，或事件的完成信号（`merge_done`）说是。
fn finished_tickets<'a>(world: &'a WorldModel, issues: &'a IssueListResponse) -> BTreeSet<&'a str> {
    issues
        .issues
        .iter()
        .filter(|issue| issue.state == IssueState::Done)
        .map(|issue| issue.issue_id.as_str())
        .chain(world.merge_done.iter().map(String::as_str))
        .collect()
}

/// 冷却中：冷却截止时刻**晚于**本 tick 的钟面。
///
/// `backoff_until` 是 `recon` 按 effective `RetryBackoff` 算出的截止时刻，与 `now` 同形
/// （宿主事件时间戳：RFC3339、UTC、秒级），所以字典序比较就是时间序比较。
fn in_cooldown(world: &WorldModel, ticket_id: &str, now: &str) -> bool {
    world
        .backoff_until
        .get(ticket_id)
        .is_some_and(|deadline| deadline.as_str() > now)
}

#[cfg(test)]
mod tests;

//! main_loop —— tick 循环的**装配**（ticket #5 / spec §2.2 + §7）。
//!
//! 一个 tick 是四段单线程顺序（ADR-0001）：`reconcile`（`crate::recon`）→
//! `decide`（`crate::tick`，纯函数、零 IO）→ `execute`（`crate::exec`），外面套一层
//! 「读本 tick 的三源 + 读生效配置 + 注入钟面」。装配的形状与每一步的读法：
//!
//! ```text
//! tick:
//!   0. GET /v1/workspace/mappings           只读：定出本 tick 的 repo_id（生效配置的键）
//!   1. GET /admin/workflow/config/{repo_id} 生效配置 + source + LKG（ADR-0004）
//!   2. GET /v1/tracker/issues               全量归一化视图（frontier / claim 的输入）
//!   3. GET /v1/events/history?since=<游标>   增量事件（并入事件账）
//!   4. reconcile                            事件账 + 视图 + mappings → WorldModel
//!   5. decide(world, issues, config, now)   纯函数
//!   6. execute                              逐 Decision 抢票 / 跑派发序列 / 交还
//! ```
//!
//! 第 0 步在第 1 步之前，是因为管理面的生效配置按 `repo_id` 取键，而 `repo_id` 在契约面
//! 只能从 workspace 映射里读出来（见下 `[UNSOURCED]`）。同一次 mappings 读数既是
//! 「本 tick 的 repo_id」的来源，也是 reconcile 的第三个源——只读一次，两处用。
//!
//! ## WS 事件任务：只入账，从不决策（ADR-0001 决策 2）
//!
//! 与主循环**并发**跑一个订阅任务（[`book_events`]）：它把宿主推来的 `EventEnvelope`
//! 追加进事件账（[`EventLedger`]，append-only、按事件 id 去重）并推进账上的游标。
//! 它**从不**调用 [`tick::decide`] 或 [`exec`]：决策的唯一路径是下一个 tick
//! 的 reconcile。所以「事件已到但决策未做」不会产生不可重放的中间态——崩溃后重建的只有
//! 事件账（ADR-0002：零持久状态）。
//!
//! ## 事件账与「增量事件」
//!
//! [`recon::rebuild`] 是**折叠整段事件**的纯函数（它每次从零推出世界），所以 tick 必须把
//! 「本窗口里的事件」整段交给它，而契约面的拉取是增量的（`since = 游标`，ticket #2 的
//! `event_watermark`）。两者由事件账调和：WS 推来的与 `history` 拉来的都**追加**进同一本
//! 账（按事件 id 去重），tick 把整本账交给 `rebuild`。增量拉取同时是 WS 掉线时的兜底——
//! 世界的权威来源仍是契约面的拉取，推送只是把同一个事实更早地记下来。
//!
//! ## 告警去重：「一次」（ticket #6）
//!
//! `recon` 的环依赖（`blocked_by_cycle`）与保留窗口截断（`history_truncated`）两条告警说的是
//! **世界的事实**，不是某一次读数：环会一直成环，宿主忙时「最近一页被截断」是常态。而
//! [`recon::rebuild`] 每次从零推世界，记不住「这条我已经说过了」——于是**发放与去重都落在
//! 本模块**（[`tick_once`] 的第 4b 步，`annotate_world_facts`）：调用方没法给自己没印发过的
//! 告警去重，所以判据（[`recon::blocked_by_cycles`] / 账上的截断位）由 `recon` 给，话由本模块说。
//! 去重位是一份逐进程的 [`AlertDedup`]，由 [`run`] 的循环持有：
//!
//! * 键 = `alert` + 同一组票（[`AlertDedup::first_sighting`]）。环上的票换了一组就是另一条
//!   告警，该说的还是要说；不带票组的告警（截断）空组即一把钥匙，整个进程只发一次。
//! * 与 ADR-0004 的 LKG「持续标注」**不是**一回事：那一条说的是「本 tick 在用什么配置」——
//!   活的配置状态，每 tick 一条（`annotate_config_state`），**不**去重。事实不随 tick 翻转
//!   才去重；「读不到某份读数」那一类也是当期事实（每个 tick 各自失败），照旧每 tick 一条。
//! * 代价（记在案）：「一次」是**曾经一次**，不是「条件每次成立时一次」。晚到的读日志的人
//!   看不到它——所以去重只用在事实类告警上，配置状态与失败类仍持续说。
//!
//! ## 钟面（`now`）
//!
//! `decide` 收一个 `now: &str`，冷却判定是**字典序**比较（ticket #3）。所以注入的钟面必须
//! 与 `EventEnvelope::occurred_at` 同形：**RFC3339、UTC、秒级**（`YYYY-MM-DDTHH:MM:SSZ`）。
//! 这个形状是宿主写事件时间戳的形状（`farm-host` 的 `now_rfc3339`），本模块用
//! [`recon`] 里那一台日历换算把本机时钟整成同形——`SystemTime::now()` 只出现在
//! [`run`] 里，**决策路径里一次也没有**。
//!
//! `[UNSOURCED]`：票面没有定「now 取宿主的钟还是编排器的钟」。取本机钟的理由是编排器没有
//! 别的钟可读（契约面没有「宿主现在几点」的读数），代价是宿主与编排器时钟偏移会平移冷却
//! 判定；两者在同一台机器上跑时偏移为 0。
//!
//! ## `[UNSOURCED]`：env 扩展（票 #5 的 env 闭集不够用）
//!
//! 票 #5 的 env 是**闭集五**（`FARM_HOST` / `FARM_TOKEN` / `FARM_ADMIN_TOKEN` /
//! `FARM_ASSIGNEE` / `TICK_INTERVAL_MS`）。但 `WorktreeEstablishRequest` 需要
//! `repo_path` / `worktree_path` / `worktree_branch`，票面与契约都没给来源；ticket #4 为此
//! 定义了显式输入类型 [`exec::WorktreeLayout`]，本票把它接到 env 上——**这是票面闭集
//! 之外的三条**，deviation 记在案：
//!
//! | 变量 | 形状 | `[UNSOURCED]` 之处 |
//! | --- | --- | --- |
//! | `FARM_REPO_PATH` | 源仓库路径（原样值） | 票面未列；无默认值 |
//! | `FARM_WORKTREE_PATH` | **模板**，如 `/srv/worktrees/{{ticket_id}}` | 票面未列；路径约定由运营者给，编排器不发明 |
//! | `FARM_WORKTREE_BRANCH` | **模板**，如 `farm/{{ticket_id}}` | 同上 |
//!
//! 后两条做成**模板**而不是固定值：`max_concurrency` 是一等配置元素，固定路径等于「一次只
//! 能跑一张票」；模板把命名约定留给运营者，编排器只借用 `{{ticket_id}}` 这一个占位符——
//! 它与 `WorkflowConfig::prompt_template` 里契约**已经**文档化的占位符是同一个，展开走
//! [`exec::expand_ticket_placeholder`] 同一个展开器（一处实现，两处调用）。
//! `spec_id` / `integration_branch` **不**加 env：它们从 `GET /v1/workspace/mappings` 读
//! （[`exec`] 已经在这么做）。
//!
//! ## `[UNSOURCED]`：repo_id
//!
//! 管理面 `GET /admin/workflow/config/{repo_id}` 按 `repo_id` 取键，而票面 env 闭集里也
//! 没有它。契约面唯一能推出 `repo_id` 的读数是 workspace 映射（`WorkspaceMapping.spec_id`），
//! 所以 [`repo_id`] 取「我手里某张票的 worktree 所属的 spec」；没有这样的映射时退到「唯一
//! 一条映射」；一条都没有、或多条并存且都与我无关时**不猜**：本 tick idle + 告警
//! （与 [`exec`] 对 mappings 推不出时的判据同款）。代价是冷启动需要一条既存映射
//! （运营者/面板建过 worktree 的 spec）——§7 的 e2e 夹具里这是显式前置。
//!
//! ## 信号
//!
//! 票面只写了「信号处理」——`[UNSOURCED]`。本票取：`SIGINT` 与 `SIGTERM` 都干净停下循环，
//! **不**在退出路上交还 claim：claim 是票的状态不是技术锁（GLOSSARY），崩溃恢复的整个故事
//! （重启 → 对账重建 → 不双派）正是建立在「claim 不随进程消失」上的，退出时主动交还反而
//! 把「我还有一张正在跑的票」这件事抹掉了。

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use farm_sdk::events::EventEnvelope;
use farm_sdk::handshake::is_compatible;
use farm_sdk::services::events::{EventHistoryQuery, EventHistoryResponse};
use farm_sdk::services::tracker::IssueListResponse;
use farm_sdk::services::workspace::MappingListResponse;
use farm_sdk::workflow::{EffectiveSource, WorkflowConfigStatus};
use farm_sdk::{ContractError, ErrorCode};
use tokio::sync::watch;

use crate::contract::client::ContractClient;
use crate::contract::ws::EventStream;
use crate::exec::{self, Outcome, ReleaseStep, WorktreeLayout};
use crate::recon::{self, WorldModel};
use crate::tick::{self, Decision, EffectiveConfig};

#[cfg(test)]
mod tests;

/// 断线之后重新订阅的等待时长。
///
/// `[UNSOURCED]`：票面与契约都没给这个常数。取 1 秒：足够短到不掉队，足够长到不把一次宿主
/// 重启变成忙等风暴。它与 `TICK_INTERVAL_MS` 无关（推送面掉了不该改变拉取面的节奏）。
const WS_RECONNECT_DELAY: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------------
// 配置
// ---------------------------------------------------------------------------

/// env 里的五条闭集变量（票 #5）与三条扩展变量（见模块头 `[UNSOURCED]`）的**名字**单一源。
pub const ENV_HOST: &str = "FARM_HOST";
pub const ENV_TOKEN: &str = "FARM_TOKEN";
pub const ENV_ADMIN_TOKEN: &str = "FARM_ADMIN_TOKEN";
pub const ENV_ASSIGNEE: &str = "FARM_ASSIGNEE";
pub const ENV_TICK_INTERVAL_MS: &str = "TICK_INTERVAL_MS";
pub const ENV_REPO_PATH: &str = "FARM_REPO_PATH";
pub const ENV_WORKTREE_PATH: &str = "FARM_WORKTREE_PATH";
pub const ENV_WORKTREE_BRANCH: &str = "FARM_WORKTREE_BRANCH";

/// `WorktreeEstablishRequest` 三条契约推不出的路径字段的**模板**（见模块头 `[UNSOURCED]`）。
///
/// 后两条是模板：`{{ticket_id}}` 由 [`exec::expand_ticket_placeholder`] 展开，所以一个进程
/// 能同时跑多张票（`max_concurrency` 是配置元素，不是 1）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorktreeLayoutTemplates {
    /// 源仓库路径（原样值，不是模板）。
    pub repo_path: String,
    /// worktree 目录路径模板。
    pub worktree_path: String,
    /// worktree 分支名模板。
    pub worktree_branch: String,
}

impl WorktreeLayoutTemplates {
    /// 展开成某一张票的 [`WorktreeLayout`]（`exec` 的输入类型）。
    pub fn layout_for(&self, ticket_id: &str) -> WorktreeLayout {
        WorktreeLayout {
            repo_path: self.repo_path.clone(),
            worktree_path: exec::expand_ticket_placeholder(&self.worktree_path, ticket_id),
            worktree_branch: exec::expand_ticket_placeholder(&self.worktree_branch, ticket_id),
        }
    }
}

/// 一个编排器进程的全部配置：五条闭集 env + 三条 `[UNSOURCED]` 扩展 env（见模块头）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrchestratorConfig {
    /// `FARM_HOST`（`host:port`）。
    pub host: SocketAddr,
    /// `FARM_TOKEN`：契约面（含 WS upgrade）的 token。
    pub token: String,
    /// `FARM_ADMIN_TOKEN`：管理面 token（读生效配置要它，ADR-0004）。
    pub admin_token: String,
    /// `FARM_ASSIGNEE`：claim 身份，与 `IssueView.assignee` 比较。
    pub assignee: String,
    /// `TICK_INTERVAL_MS`：tick 间隔。
    pub tick_interval: Duration,
    /// worktree 三条路径的模板。
    pub layout: WorktreeLayoutTemplates,
}

/// 配置装载失败（闭集）。一律**指名字段**，绝不静默用默认值兜底。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigError {
    /// 变量缺失（没设，或设成了空串）。
    Missing(&'static str),
    /// 变量在，但形状不是契约面要的（坏地址、坏整数、0）。
    Invalid {
        var: &'static str,
        value: String,
        expected: &'static str,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Missing(var) => write!(f, "{var} is required but is not set"),
            ConfigError::Invalid {
                var,
                value,
                expected,
            } => write!(f, "{var}={value:?} is not {expected}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl OrchestratorConfig {
    /// 从进程环境装载（`main` 的入口）。
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// 从一个查名函数装载（单测用它，避免动进程级的 env）。
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let host = required(&lookup, ENV_HOST)?;
        let host = host
            .parse::<SocketAddr>()
            .map_err(|_| ConfigError::Invalid {
                var: ENV_HOST,
                value: host.clone(),
                expected: "a host:port socket address",
            })?;
        let raw_interval = required(&lookup, ENV_TICK_INTERVAL_MS)?;
        let tick_interval_ms = raw_interval
            .parse::<u64>()
            .ok()
            .filter(|ms| *ms > 0)
            .ok_or_else(|| ConfigError::Invalid {
                var: ENV_TICK_INTERVAL_MS,
                value: raw_interval.clone(),
                expected: "a positive whole number of milliseconds",
            })?;
        Ok(Self {
            host,
            token: required(&lookup, ENV_TOKEN)?,
            admin_token: required(&lookup, ENV_ADMIN_TOKEN)?,
            assignee: required(&lookup, ENV_ASSIGNEE)?,
            tick_interval: Duration::from_millis(tick_interval_ms),
            layout: WorktreeLayoutTemplates {
                repo_path: required(&lookup, ENV_REPO_PATH)?,
                worktree_path: required(&lookup, ENV_WORKTREE_PATH)?,
                worktree_branch: required(&lookup, ENV_WORKTREE_BRANCH)?,
            },
        })
    }
}

/// 取一条必填变量：缺失与空串都算缺（空串是「设了个空」，不是「给了值」）。
fn required(
    lookup: &impl Fn(&str) -> Option<String>,
    var: &'static str,
) -> Result<String, ConfigError> {
    match lookup(var) {
        Some(value) if !value.is_empty() => Ok(value),
        _ => Err(ConfigError::Missing(var)),
    }
}

// ---------------------------------------------------------------------------
// 事件账（ADR-0001 决策 2）
// ---------------------------------------------------------------------------

/// 事件账：按 `occurred_at` **定序**、按事件 id 去重、游标 = 账上最新事件的 `occurred_at`。
///
/// WS 任务与 tick 的增量拉取都往这里追加（两路是同一个事实，去重后只留一份）。它是**账**
/// 不是世界：`WorldModel` 仍然只由下一个 tick 的 reconcile 推出（ADR-0001 决策 3）。
///
/// **顺序是账的职责**：两路来源以**到达顺序**混进来（WS 一条一条推、HTTP 一次一页拉），
/// 而 `recon::rebuild` 的折叠要求宿主时间序（`occurred_at` 升序）——它不是调用方能给的：
/// WS 推来的是整个保留窗口，HTTP 拉来的是最近一页，两边的区间可以交错甚至倒挂。所以入账
/// 就地插进有序位置，账上的顺序恒是时间序。
#[derive(Debug, Default)]
pub struct EventLedger {
    events: Vec<EventEnvelope>,
    seen: std::collections::BTreeSet<String>,
    /// **当前**那一页增量拉取的读数：它是不是被保留窗口截断。
    ///
    /// 不是历史累积（一次截断不该把退化路径钉成稳态）：下一次不截断的拉取就把它清掉。
    truncated: bool,
}

impl EventLedger {
    /// 入账一条事件：已见过的事件 id 是幂等空操作（WS 快照与增量拉取会交叠）。
    pub fn append(&mut self, envelope: EventEnvelope) {
        if !self.seen.insert(envelope.id.clone()) {
            return;
        }
        // 保持 `occurred_at` 升序（稳定插入：与它同一时刻的都已排在它前面，故同一秒内仍然是
        // 到达顺序——契约只给到秒级，再细的序推不出就不推）。
        let position = self
            .events
            .partition_point(|existing| existing.occurred_at <= envelope.occurred_at);
        self.events.insert(position, envelope);
    }

    /// 入账一页事件历史（增量拉取的读数）。
    ///
    /// 截断位每次拉取**重取**（取当前那一页的读数），不 OR 进账里：宿主忙时「最近一页被截断」
    /// 是常态，钉住它等于把 `recon` 的退化路径（完成信号回退 issues 快照）钉成稳态。
    pub fn absorb(&mut self, page: &EventHistoryResponse) {
        for envelope in &page.events {
            self.append(envelope.clone());
        }
        self.truncated = page.truncated;
    }

    /// 交给 `recon::rebuild` 的那一段：整本账（已按 `occurred_at` 升序）。
    pub fn page(&self) -> EventHistoryResponse {
        EventHistoryResponse {
            events: self.events.clone(),
            truncated: self.truncated,
        }
    }

    /// 账上**最新**事件的 `occurred_at`（`None` = 账还是空的）——下一次增量拉取的 `since` 下界。
    pub fn watermark(&self) -> Option<&str> {
        self.events.last().map(|event| event.occurred_at.as_str())
    }

    /// 已入账的事件条数（机械断言用）。
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// 账上一条事件都没有。
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

// ---------------------------------------------------------------------------
// 告警去重（ticket #6）
// ---------------------------------------------------------------------------

/// 逐进程的告警去重账：**同一 `alert` + 同一组票只发一次**。
///
/// 它由 [`run`] 的循环持有（一个进程一份），跟着 tick 一路传下去。为什么去重位只能在调用方：
/// [`recon::rebuild`] 每次从零推世界（纯函数、零持久状态，ADR-0002），它必然每次都重新推出
/// 同一个环、重新看到同一个截断位；而调用方没法给自己**没印发过**的告警去重——所以发放也
/// 一并在调用方（`annotate_world_facts`），`recon` 只出判据。
///
/// 去重的是**世界事实**类告警（见模块头「告警去重」）；配置状态标注（ADR-0004 的 LKG）与
/// 「读不到」类失败都是当期事实，照旧每 tick 一条。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AlertDedup {
    /// 已发过的键：`alert` + `|` + 票组（空票组 = 这条告警不带票组）。
    seen: BTreeSet<String>,
}

impl AlertDedup {
    /// 一本空账（一条告警都还没发过）。
    pub fn new() -> Self {
        Self::default()
    }

    /// 这条告警在本进程里是不是**第一次**露面：是 ⇒ 登记并返回 `true`（调用方发那条 WARN）。
    ///
    /// 键带票组：环上的票换了一组就是另一条告警（该说的还是要说）；不带票组的告警传空串，
    /// 空组即一把钥匙，整个进程只发一次。
    pub fn first_sighting(&mut self, alert: &str, tickets: &str) -> bool {
        self.seen.insert(format!("{alert}|{tickets}"))
    }

    /// 已发过几条（机械断言用）。
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    /// 一条都还没发过。
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

// ---------------------------------------------------------------------------
// 一次 tick
// ---------------------------------------------------------------------------

/// 一条决策的执行结局（闭集）。它让「装配是否真的按决策打契约调用」可观察。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TickOutcome {
    /// `Decision::Claim` 拿到了票。
    Claimed { ticket_id: String },
    /// `Decision::Claim` 没拿到（票被他人持有 / 契约拒绝）。
    ClaimRefused { ticket_id: String, code: ErrorCode },
    /// `Decision::Dispatch` 的派发序列结局（`exec` 的闭集原样带出）。
    Dispatch { ticket_id: String, outcome: Outcome },
    /// `Decision::Release` 的结局（`exec` 的闭集原样带出）。
    Release {
        ticket_id: String,
        step: ReleaseStep,
    },
}

/// 一次 tick 的可观察结果。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TickReport {
    /// 本 tick 生效配置的来源；`None` = 没读到配置（repo_id 推不出 / 管理面不可达）。
    pub source: Option<EffectiveSource>,
    /// 本 tick 的决策表（[`tick::decide`] 的输出）。
    pub decisions: Vec<Decision>,
    /// 逐决策的执行结局（与决策表同序）。
    pub outcomes: Vec<TickOutcome>,
    /// 本 tick 是否被一条硬失败**截断**：遇到 [`Outcome::HardFailure`] 就不再发余下的
    /// 契约调用（`exec` 的 `Disposition::Fatal` 语义，见 `exec` 模块头的分流表）。
    /// 值是截断本 tick 的那张票——`None` = 没被截断。
    pub cut_short_by: Option<String>,
}

/// 跑一个完整的 tick（模块头的六步）。
///
/// `world` 是上一个 tick 重建出的世界（就地刷新）；`alerts` 是本进程的告警去重账
/// （ticket #6，见模块头「告警去重」）；`now` 是本 tick 注入的钟面（RFC3339、UTC、秒级，
/// 见模块头）。读不到某一份读数时**不猜**：告警 + 本 tick 不编排。
///
/// 世界里的 `backoff_until` 是**两路**冷却的并集：事件流折叠出的（`recon`）与 `exec` 失败
/// 回滚交回的（本模块折进，见函数里的第 6b 步）。
pub async fn tick_once(
    client: &ContractClient,
    config: &OrchestratorConfig,
    ledger: &Mutex<EventLedger>,
    world: &mut WorldModel,
    alerts: &mut AlertDedup,
    now: &str,
    tick: u64,
) -> TickReport {
    // 0. workspace 映射：既是 repo_id 的来源，也是 reconcile 的第三个源（只读一次）。
    let mappings = match client.mappings().await {
        Ok(mappings) => mappings,
        Err(error) => {
            tracing::warn!(
                alert = "mappings_unavailable",
                code = error.code.as_str(),
                tick,
                "workspace 映射读不到：本 tick 不编排"
            );
            return TickReport::default();
        }
    };

    // 0b. repo_id：只能从映射推（模块头 `[UNSOURCED]`）。
    let Some(repo_id) = repo_id(&mappings, world) else {
        tracing::warn!(
            alert = "repo_id_underivable",
            tick,
            "workspace 映射里挑不出本编排器的 repo：本 tick idle（不猜配置归属）"
        );
        return TickReport::default();
    };

    // 1. 生效配置（管理面，独立 admin token）。
    let status = match client.workflow_config(repo_id).await {
        Ok(status) => status,
        Err(error) => {
            tracing::warn!(
                alert = "workflow_config_unavailable",
                code = error.code.as_str(),
                repo_id,
                tick,
                "生效配置读不到：本 tick 不编排"
            );
            return TickReport::default();
        }
    };
    let effective = EffectiveConfig::from_status(&status);
    annotate_config_state(&effective, &status, repo_id, tick);

    // 2. 归一化视图（全量快照）。
    let issues = match client.issues().await {
        Ok(issues) => issues,
        Err(error) => {
            tracing::warn!(
                alert = "issues_unavailable",
                code = error.code.as_str(),
                tick,
                "归一化视图读不到：本 tick 不编排"
            );
            return TickReport::default();
        }
    };

    // 3. 增量事件：`since` = 上一个世界的游标。
    let query = EventHistoryQuery {
        since: world.event_watermark.clone(),
        ..Default::default()
    };
    match client.event_history(&query).await {
        Ok(page) => ledger.lock().expect("事件账锁未被毒化").absorb(&page),
        Err(error) => tracing::warn!(
            alert = "event_history_unavailable",
            code = error.code.as_str(),
            tick,
            "增量事件读不到：本 tick 用账上已有的事件继续（WS 推来的仍可用）"
        ),
    }

    // 4. reconcile：整本账 + 视图 + 映射 → 世界。
    //
    // 冷却登记是**累积**的：还没到期的截止时刻不因重建（只看得见事件）而消失，因为
    // `exec` 的失败回滚记的冷却在事件面没有对应事件（宿主不会为一次没起来的 dispatch 发
    // 事件）。这里把上一个世界里还没到期的截止时刻搬过来，取两者中更晚的那个。
    let carried: Vec<(String, String)> = world
        .backoff_until
        .iter()
        .filter(|(_, deadline)| deadline.as_str() > now)
        .map(|(ticket, deadline)| (ticket.clone(), deadline.clone()))
        .collect();
    let (page, ledger_watermark) = {
        let account = ledger.lock().expect("事件账锁未被毒化");
        (account.page(), account.watermark().map(str::to_string))
    };
    let previous_watermark = world.event_watermark.clone();
    let retry_backoff = effective
        .effective
        .as_ref()
        .map(|workflow| workflow.retry_backoff.clone())
        .unwrap_or_default();
    *world = recon::rebuild(
        &config.assignee,
        &issues,
        &page,
        previous_watermark.as_deref(),
        &retry_backoff,
    );
    for (ticket_id, deadline) in carried {
        let kept = world.backoff_until.entry(ticket_id).or_default();
        if *kept < deadline {
            *kept = deadline;
        }
    }

    // 4b. recon 的两条「世界事实」告警：判据在手，发放受 `alerts` 去重（ticket #6）。
    annotate_world_facts(alerts, &issues, &page, &mappings, tick);

    // 5. decide（纯函数；`now` 是注入的钟面，不在这里取时钟）。
    let decisions = tick::decide(world, &issues, &effective, now);

    // 6. execute：逐决策打契约调用。ADR-0004：`effective == None` ⇒ 无执行面（idle）。
    let mut outcomes = Vec::with_capacity(decisions.len());
    let mut cut_short_by: Option<String> = None;
    if let Some(workflow) = effective.effective.clone() {
        for decision in &decisions {
            let outcome = match decision {
                Decision::Claim { ticket_id } => {
                    // `claimed == false` 的解释只有一处（`exec::interpret_claim`）：契约说这就是
                    // 「已被他人持有」。
                    match client.claim(ticket_id, &config.assignee).await {
                        Ok(response) => match exec::interpret_claim(&response) {
                            Ok(()) => TickOutcome::Claimed {
                                ticket_id: ticket_id.clone(),
                            },
                            Err(code) => TickOutcome::ClaimRefused {
                                ticket_id: ticket_id.clone(),
                                code,
                            },
                        },
                        Err(error) => TickOutcome::ClaimRefused {
                            ticket_id: ticket_id.clone(),
                            code: error.code,
                        },
                    }
                }
                Decision::Dispatch { ticket_id } => {
                    let layout = config.layout.layout_for(ticket_id);
                    TickOutcome::Dispatch {
                        ticket_id: ticket_id.clone(),
                        outcome: exec::dispatch_ticket(
                            client,
                            &config.assignee,
                            ticket_id,
                            &workflow,
                            &layout,
                        )
                        .await,
                    }
                }
                Decision::Release { ticket_id } => TickOutcome::Release {
                    ticket_id: ticket_id.clone(),
                    step: exec::release_ticket(client, &config.assignee, ticket_id, &workflow)
                        .await,
                },
            };
            tracing::debug!(
                tick,
                ticket = match &decision {
                    Decision::Claim { ticket_id }
                    | Decision::Dispatch { ticket_id }
                    | Decision::Release { ticket_id } => ticket_id.as_str(),
                },
                decision = ?decision,
                outcome = ?outcome,
                "decision executed"
            );
            let fatal = is_fatal(&outcome);
            let ticket = outcome_ticket(&outcome).to_string();
            outcomes.push(outcome);
            // `Disposition::Fatal` 的语义（`exec` 模块头分流表）：硬失败之后，本 tick 余下的
            // 序列**不再发契约调用**。凭据/契约版本/路由这类失败对每个决策都会同样失败，
            // 继续打只是把同一个错误重复 N 次、并把 N 张票都拖进回滚。
            if fatal {
                tracing::warn!(
                    alert = "tick_cut_short",
                    ticket = ticket.as_str(),
                    "硬失败：本 tick 余下的序列不再发契约调用"
                );
                cut_short_by = Some(ticket);
                break;
            }
        }
    }

    // 6b. 冷却折进世界：`exec` 的失败回滚交回的 [`exec::Cooldown`] 是**本 tick 的事实**，
    //     事件面看不见它，所以在这里按 effective `RetryBackoff` 换成截止时刻写进
    //     `backoff_until`（换算是 `recon` 那一处）；下一个 tick 的 decide 因此看得到它，
    //     这张票在冷却到期前不会被重新抢 / 重新派发（ticket #4：「非 service_unavailable
    //     失败 ⇒ release + 记冷却」）。
    for outcome in &outcomes {
        if let Some(cooldown) = cooldown_of(outcome) {
            world.backoff_until.insert(
                cooldown.ticket_id.clone(),
                recon::cooldown_deadline(now, cooldown.delay_ms),
            );
        }
    }

    tracing::debug!(
        tick,
        source = ?effective.source,
        decisions = decisions.len(),
        ledger_watermark = ledger_watermark.as_deref().unwrap_or(""),
        world_watermark = world.event_watermark.as_deref().unwrap_or(""),
        "tick"
    );

    TickReport {
        source: effective.source,
        decisions,
        outcomes,
        cut_short_by,
    }
}

/// 这条结局是不是「本 tick 余下的序列不再发契约调用」的那一种。
fn is_fatal(outcome: &TickOutcome) -> bool {
    matches!(
        outcome,
        TickOutcome::Dispatch {
            outcome: Outcome::HardFailure { .. },
            ..
        }
    )
}

/// 一条执行结局针对的那张票。
fn outcome_ticket(outcome: &TickOutcome) -> &str {
    match outcome {
        TickOutcome::Claimed { ticket_id }
        | TickOutcome::ClaimRefused { ticket_id, .. }
        | TickOutcome::Dispatch { ticket_id, .. }
        | TickOutcome::Release { ticket_id, .. } => ticket_id,
    }
}

/// 一条执行结局里带的冷却登记（没有冷却的结局返回 `None`）。
///
/// 冷却只出现在两个地方：`exec` 的失败回滚（[`Outcome::Abandoned`] / [`Outcome::HardFailure`]
/// 的 `rollback.cooldown`）与交还硬失败（[`ReleaseStep::Failed`] 的 `cooldown`）。
fn cooldown_of(outcome: &TickOutcome) -> Option<&exec::Cooldown> {
    match outcome {
        TickOutcome::Claimed { .. } | TickOutcome::ClaimRefused { .. } => None,
        TickOutcome::Dispatch { outcome, .. } => match outcome {
            Outcome::Abandoned { rollback, .. } | Outcome::HardFailure { rollback, .. } => {
                Some(&rollback.cooldown)
            }
            Outcome::Dispatched { .. } | Outcome::Retried { .. } | Outcome::LayoutUnderivable => {
                None
            }
        },
        TickOutcome::Release { step, .. } => match step {
            ReleaseStep::Failed { cooldown, .. } => Some(cooldown),
            ReleaseStep::Released { .. } | ReleaseStep::Retried { .. } => None,
        },
    }
}

/// repo_id（生效配置的键）：先按「我名下票的 worktree 所属 spec」，退到「唯一一条映射」。
///
/// 挑映射的判据只有一处（[`exec::select_mapping`]）：先按票挑、唯一映射兜底、多条并存不猜。
/// 推不出就返回 `None`：本 tick idle + 告警，不猜配置归属。
fn repo_id<'a>(mappings: &'a MappingListResponse, world: &WorldModel) -> Option<&'a str> {
    exec::select_mapping(mappings, |ticket| world.claimed_by_me.contains(ticket))
        .map(|mapping| mapping.spec_id.as_str())
}

/// ADR-0004 的三态标注（只读该端点，从不尝试写配置）：
///
/// | `effective` | `source` | 行为 |
/// | --- | --- | --- |
/// | `None` | 任意 | idle + 告警；**不**自造默认配置 |
/// | `Some` | `fresh` | 正常编排，不标注 |
/// | `Some` | `last_known_good` | 正常编排 + **持续标注**（每 tick 一条，不刷屏也漏不了） |
fn annotate_config_state(
    effective: &EffectiveConfig,
    status: &WorkflowConfigStatus,
    repo_id: &str,
    tick: u64,
) {
    match (&effective.effective, effective.source) {
        (None, _) => tracing::warn!(
            alert = "workflow_config_refused",
            repo_id,
            tick,
            last_alert = status.last_alert.as_deref().unwrap_or(""),
            "尚无生效配置（首次即非法）：idle，不编排——不自造默认配置（ADR-0004）"
        ),
        (Some(workflow), Some(EffectiveSource::LastKnownGood)) => tracing::warn!(
            alert = "workflow_config_last_known_good",
            repo_id,
            tick,
            max_concurrency = workflow.max_concurrency,
            loaded_at = status.loaded_at.as_deref().unwrap_or(""),
            last_known_good_at = status
                .last_known_good
                .as_ref()
                .map(|info| info.updated_at.as_str())
                .unwrap_or(""),
            last_alert = status.last_alert.as_deref().unwrap_or(""),
            "在用 last-known-good 配置：照常编排，标注持续有效（ADR-0004）"
        ),
        (Some(_), _) => {}
    }
}

/// `recon` 的两条「世界事实」告警：**同一 `alert` + 同一组票只发一次**（ticket #6）。
///
/// 与 `annotate_config_state` 的分界：那一条说的是「本 tick 在用什么配置」——LKG 是活的配置
/// 状态，ADR-0004 要求每 tick 一条；这两条说的是**世界的事实**（环会一直成环、宿主忙时最近
/// 一页会一直被截断），事实不随 tick 翻转，所以去重。反过来，「读不到某份读数」是当期事实
/// （每个 tick 各自失败），不去重。
///
/// 判据不在这里算：环是 [`recon::blocked_by_cycles`] 从归一化视图推的；截断位是事件账上
/// **当前那一页**的读数（[`EventLedger::absorb`] 每次拉取重取，不粘——粘住就等于把 `recon`
/// 的退化路径钉成稳态）。
fn annotate_world_facts(
    alerts: &mut AlertDedup,
    issues: &IssueListResponse,
    page: &EventHistoryResponse,
    mappings: &MappingListResponse,
    tick: u64,
) {
    for group in recon::blocked_by_cycles(issues) {
        let tickets = group.join(",");
        if alerts.first_sighting("blocked_by_cycle", &tickets) {
            tracing::warn!(
                alert = "blocked_by_cycle",
                tickets = tickets.as_str(),
                tick,
                "blocked_by 成环：环上的票永远不会 dispatchable"
            );
        }
    }
    if page.truncated && alerts.first_sighting("history_truncated", "") {
        tracing::warn!(
            alert = "history_truncated",
            mappings = mappings.mappings.len(),
            tick,
            "事件历史被保留窗口截断：完成信号回退 issues 快照 + workspace mappings"
        );
    }
}

// ---------------------------------------------------------------------------
// 主循环
// ---------------------------------------------------------------------------

/// 主循环的致命错误（**只有**装配期的错是致命的；tick 内的读写失败是告警 + 下一 tick 重试）。
#[derive(Debug)]
pub enum RunError {
    /// 握手失败（契约面不可达 / 契约拒绝）。
    Handshake(ContractError),
    /// 契约主版本不兼容（握手成功，但宿主不支持本客户端实现的契约主版本）。
    IncompatibleContract {
        host: String,
        supported: Vec<String>,
    },
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::Handshake(error) => write!(f, "the contract handshake failed: {error}"),
            RunError::IncompatibleContract { host, supported } => write!(
                f,
                "the host speaks contract {host} (supported {supported:?}); this client implements {}",
                farm_sdk::CONTRACT_VERSION
            ),
        }
    }
}

impl std::error::Error for RunError {}

/// 装配并跑主循环，直到 `shutdown` 变成 `true`。
///
/// 退出路上**不**交还 claim（见模块头「信号」）：claim 是票的状态，不是进程的锁。
pub async fn run(
    config: OrchestratorConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), RunError> {
    let client = ContractClient::new(
        config.host,
        config.token.clone(),
        Some(config.admin_token.clone()),
    );
    let handshake = client.handshake().await.map_err(RunError::Handshake)?;
    if !is_compatible(&handshake.contract_version) {
        return Err(RunError::IncompatibleContract {
            host: handshake.contract_version,
            supported: handshake.supported_contract_versions,
        });
    }
    tracing::info!(
        host = %config.host,
        host_version = handshake.host_version.as_str(),
        contract_version = handshake.contract_version.as_str(),
        assignee = config.assignee.as_str(),
        tick_interval_ms = config.tick_interval.as_millis(),
        "handshake ok; the tick loop starts"
    );

    // WS 事件任务：只入账（ADR-0001）。它拿自己的 shutdown 句柄，主循环退出时一起收尾。
    let ledger = Arc::new(Mutex::new(EventLedger::default()));
    let events = tokio::spawn(book_events(
        config.host,
        config.token.clone(),
        Arc::clone(&ledger),
        shutdown.clone(),
    ));

    let mut world = WorldModel::default();
    // 告警去重账：一个进程一份，跟着 tick 走（ticket #6）。
    let mut alerts = AlertDedup::default();
    let mut tick = 0u64;
    loop {
        tick += 1;
        // 钟面在**这一处**取，然后作为字符串注入决策路径（模块头「钟面」）。
        let now = host_timestamp(SystemTime::now());
        tick_once(
            &client,
            &config,
            &ledger,
            &mut world,
            &mut alerts,
            &now,
            tick,
        )
        .await;

        tokio::select! {
            _ = tokio::time::sleep(config.tick_interval) => {}
            _ = shutdown.changed() => break,
        }
        if *shutdown.borrow() {
            break;
        }
    }

    events.abort();
    tracing::info!(ticks = tick, "shutdown signalled; the tick loop stops");
    Ok(())
}

/// WS 任务：订阅 `GET /v1/events/stream`，把事件信封**追加进事件账**并推进游标。
///
/// 它不读世界、不做决策、不发契约调用——除了订阅本身。掉线按 [`WS_RECONNECT_DELAY`]
/// 重订阅：推送面掉了不该让世界停更（tick 的增量拉取仍在给世界供数）。
async fn book_events(
    host: SocketAddr,
    token: String,
    ledger: Arc<Mutex<EventLedger>>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        match subscribe(host, &token, &ledger, &mut shutdown).await {
            Ok(()) => return, // 收到关闭信号
            Err(error) => tracing::warn!(
                alert = "events_stream_lost",
                error = error.to_string().as_str(),
                "事件流断开：按 WS_RECONNECT_DELAY 重订阅（拉取面仍在给世界供数）"
            ),
        }
        tokio::select! {
            _ = tokio::time::sleep(WS_RECONNECT_DELAY) => {}
            _ = shutdown.changed() => return,
        }
    }
}

/// 一条订阅的生命周期：upgrade → `hello`（校验契约主版本）→ 帧流入账。
///
/// `Ok(())` = 收到关闭信号或对端正常关闭；`Err` = 这条订阅断了（调用方决定要不要重订阅）。
async fn subscribe(
    host: SocketAddr,
    token: &str,
    ledger: &Mutex<EventLedger>,
    shutdown: &mut watch::Receiver<bool>,
) -> std::io::Result<()> {
    let mut stream = EventStream::connect(host, token).await?;
    let hello = stream.hello().await?;
    if !is_compatible(&hello.contract_version) {
        return Err(std::io::Error::other(format!(
            "the event stream speaks contract {} which this client cannot consume",
            hello.contract_version
        )));
    }
    tracing::info!(
        contract_version = hello.contract_version.as_str(),
        event_kinds = hello.event_kinds.len(),
        "event stream subscription established (bookkeeping only)"
    );
    loop {
        tokio::select! {
            frame = stream.next_envelope() => match frame? {
                Some(envelope) => ledger.lock().expect("事件账锁未被毒化").append(envelope),
                None => return Ok(()), // 对端正常关闭
            },
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 钟面
// ---------------------------------------------------------------------------

/// 本机时钟 → **宿主事件时间戳形状**（RFC3339、UTC、秒级）。
///
/// 日历换算复用 [`recon`] 里那一台（`format_host_timestamp`）：形状只有一处定义，「字典序
/// 即时间序」的前提就只有一处需要成立。`SystemTime::now()` 只出现在 [`run`] 里，决策路径
/// （[`tick::decide`] / [`recon::rebuild`]）收到的是这个字符串。
pub fn host_timestamp(at: SystemTime) -> String {
    let epoch_secs = match at.duration_since(UNIX_EPOCH) {
        Ok(since) => i64::try_from(since.as_secs()).unwrap_or(i64::MAX),
        // 1970 之前（本机时钟设在过去）：取负秒数，形状仍然合法。
        Err(before) => -i64::try_from(before.duration().as_secs()).unwrap_or(i64::MAX),
    };
    recon::format_host_timestamp(epoch_secs)
}

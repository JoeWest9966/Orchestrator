//! `main_loop` 的单测：内存内假宿主 + 假 WS 宿主，测的是**装配的可观察行为**——
//! 一个 tick 打了哪些契约调用、按什么顺序、决策表与执行结局是什么、ADR-0004 的三态各
//! 自打了什么标注，以及 WS 任务是否**只入账**。
//!
//! 期望值一律取契约面字面量（`farm-sdk` 类型 / 已知常量），不拿实现重算。

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use farm_sdk::events::{EventEnvelope, EventKind, WsHello};
use farm_sdk::handshake::HandshakeResponse;
use farm_sdk::services::agent_runner::{AttemptState, DispatchResponse};
use farm_sdk::services::events::EventHistoryResponse;
use farm_sdk::services::tracker::{
    ClaimResponse, IssueListResponse, IssueState, IssueView, ReleaseOutcome, ReleaseResponse,
};
use farm_sdk::services::workspace::{
    MappingListResponse, WorkspaceMapping, WorktreeRef, WorktreeState,
};
use farm_sdk::workflow::{EffectiveSource, RetryBackoff, WorkflowConfig, WorkflowConfigStatus};
use farm_sdk::{contract_path, routes, ServiceName};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing_subscriber::layer::SubscriberExt;

use super::*;
use crate::contract::ws::EventStream;
use crate::exec::Outcome;
use crate::tick::Decision;

const REPO: &str = "spec-1";
const INTEGRATION: &str = "feat/spec-1";
const TICKET: &str = "T1";
const ASSIGNEE: &str = "orch-1";
const ATTEMPT: &str = "attempt-1";

// ---------------------------------------------------------------------------
// 假宿主（HTTP）
// ---------------------------------------------------------------------------

fn json<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("response serializes")
}

/// 假宿主的一条读数：`(方法, 路径不含查询串) → (HTTP 状态码, 响应体)`。
type Routes = BTreeMap<String, (u16, String)>;

struct StubHost {
    addr: SocketAddr,
    routes: Arc<Mutex<Routes>>,
    requests: Arc<Mutex<Vec<(String, String)>>>,
}

impl StubHost {
    async fn spawn(routes: Routes) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("stub addr");
        let shared = Arc::new(Mutex::new(routes));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let served = Arc::clone(&shared);
        let recorded = Arc::clone(&requests);
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let served = Arc::clone(&served);
                let recorded = Arc::clone(&recorded);
                tokio::spawn(async move {
                    let (method, path, _) = read_request(&mut socket).await;
                    // WS upgrade 不在这台假宿主上：关掉连接，`main_loop` 的订阅任务会告警并重连。
                    if !path.starts_with("/v1/")
                        && !path.starts_with("/admin/")
                        && path != "/handshake"
                    {
                        let _ = socket.shutdown().await;
                        return;
                    }
                    recorded
                        .lock()
                        .unwrap()
                        .push((method.clone(), path.clone()));
                    let body = served
                        .lock()
                        .unwrap()
                        .get(&format!("{method} {path}"))
                        .cloned();
                    let response = match body {
                        Some((status, body)) => format!(
                            "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\n\
                             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        ),
                        None => {
                            let error = farm_sdk::ContractError::not_found(format!(
                                "stub has no route for {method} {path}"
                            ));
                            let body = json(&error);
                            format!(
                                "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n\
                                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            )
                        }
                    };
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.flush().await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        Self {
            addr,
            routes: shared,
            requests,
        }
    }

    fn set<T: serde::Serialize>(&self, method: &str, path: &str, body: &T) {
        self.routes
            .lock()
            .unwrap()
            .insert(format!("{method} {path}"), (200, json(body)));
    }

    /// 一条**失败**读数：闭集错误码的 HTTP 状态码 + 契约错误信封。
    fn set_error(&self, method: &str, path: &str, code: ErrorCode) {
        self.routes.lock().unwrap().insert(
            format!("{method} {path}"),
            (
                code.http_status(),
                ContractError::new(code, "scripted failure").to_json(),
            ),
        );
    }

    fn request_log(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|(method, path)| format!("{method} {path}"))
            .collect()
    }

    fn counts(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for entry in self.request_log() {
            *counts.entry(entry).or_insert(0) += 1;
        }
        counts
    }
}

async fn read_request(socket: &mut TcpStream) -> (String, String, String) {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        let read = socket.read(&mut byte).await.expect("read request");
        if read == 0 {
            break;
        }
        buf.extend_from_slice(&byte[..read]);
    }
    let head = String::from_utf8_lossy(&buf).into_owned();
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();
    let path = target.split('?').next().unwrap_or_default().to_string();
    let content_length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        socket.read_exact(&mut body).await.expect("read body");
    }
    (method, path, String::from_utf8_lossy(&body).into_owned())
}

// ---------------------------------------------------------------------------
// 契约面的夹具读数
// ---------------------------------------------------------------------------

fn config(
    status_effective: Option<WorkflowConfig>,
    source: Option<EffectiveSource>,
) -> WorkflowConfigStatus {
    WorkflowConfigStatus {
        repo_id: REPO.to_string(),
        effective: status_effective,
        source,
        loaded_at: Some("2026-01-01T00:00:00Z".to_string()),
        last_known_good: Some(farm_sdk::workflow::LastKnownGoodInfo {
            updated_at: "2025-12-31T00:00:00Z".to_string(),
        }),
        last_alert: Some("invalid workflow.json".to_string()),
    }
}

fn workflow() -> WorkflowConfig {
    WorkflowConfig {
        dispatch_labels: vec!["dispatch".to_string()],
        max_concurrency: 1,
        stall_threshold_secs: 3_600,
        prompt_template: "work on {{ticket_id}}".to_string(),
        retry_backoff: RetryBackoff {
            max_retries: 2,
            base_delay_ms: 1_000,
            backoff_multiplier: 2,
        },
        projection: Default::default(),
    }
}

fn issue(assignee: Option<&str>, state: IssueState) -> IssueView {
    IssueView {
        issue_id: TICKET.to_string(),
        title: "ticket".to_string(),
        state,
        labels: vec!["dispatch".to_string()],
        assignee: assignee.map(str::to_string),
        updated_at: None,
        blocked_by: Vec::new(),
        blocking: Vec::new(),
    }
}

fn issues(assignee: Option<&str>, state: IssueState) -> IssueListResponse {
    IssueListResponse {
        issues: vec![issue(assignee, state)],
    }
}

fn mappings(worktrees: Vec<WorktreeRef>) -> MappingListResponse {
    MappingListResponse {
        mappings: vec![WorkspaceMapping {
            spec_id: REPO.to_string(),
            integration_branch: INTEGRATION.to_string(),
            worktrees,
        }],
    }
}

fn ticket_worktree() -> WorktreeRef {
    WorktreeRef {
        ticket_id: TICKET.to_string(),
        path: "/worktrees/T1".to_string(),
        branch: "farm/T1".to_string(),
        state: WorktreeState::Ready,
    }
}

fn empty_history() -> EventHistoryResponse {
    EventHistoryResponse {
        events: Vec::new(),
        truncated: false,
    }
}

fn merge_event() -> EventEnvelope {
    EventEnvelope {
        id: "e-merge".to_string(),
        kind: EventKind::Merge,
        occurred_at: "2026-01-01T00:00:05Z".to_string(),
        actor: "merge".to_string(),
        ticket_id: Some(TICKET.to_string()),
        attempt_id: None,
        payload: serde_json::Value::Null,
        audited: false,
    }
}

/// 一张票的某类事件（宿主事件时间戳形状的时间）。
fn ticket_event(id: &str, kind: EventKind, occurred_at: &str, attempt_id: &str) -> EventEnvelope {
    EventEnvelope {
        id: id.to_string(),
        kind,
        occurred_at: occurred_at.to_string(),
        actor: "host".to_string(),
        ticket_id: Some(TICKET.to_string()),
        attempt_id: Some(attempt_id.to_string()),
        payload: serde_json::Value::Null,
        audited: false,
    }
}

fn test_config(addr: SocketAddr) -> OrchestratorConfig {
    OrchestratorConfig {
        host: addr,
        token: "op".to_string(),
        admin_token: "admin".to_string(),
        assignee: ASSIGNEE.to_string(),
        tick_interval: Duration::from_millis(50),
        layout: WorktreeLayoutTemplates {
            repo_path: "/repos/farm".to_string(),
            worktree_path: "/worktrees/{{ticket_id}}".to_string(),
            worktree_branch: "farm/{{ticket_id}}".to_string(),
        },
    }
}

/// 把假宿主的全部契约读数装进去（握手 + 映射 + 配置 + 视图 + 事件 + 三条写路由）。
async fn ready_host(
    status: WorkflowConfigStatus,
    issues: IssueListResponse,
    history: EventHistoryResponse,
    worktrees: Vec<WorktreeRef>,
) -> StubHost {
    let host = StubHost::spawn(BTreeMap::new()).await;
    host.set(
        "POST",
        "/handshake",
        &HandshakeResponse::for_host("stub-host"),
    );
    host.set(
        "GET",
        &contract_path(ServiceName::Workspace, "/mappings"),
        &mappings(worktrees),
    );
    host.set(
        "GET",
        &routes::admin_path(&format!("/workflow/config/{REPO}")),
        &status,
    );
    host.set(
        "GET",
        &contract_path(ServiceName::Tracker, "/issues"),
        &issues,
    );
    host.set(
        "GET",
        &contract_path(ServiceName::Events, "/history"),
        &history,
    );
    host.set(
        "POST",
        &contract_path(ServiceName::Tracker, "/claims"),
        &ClaimResponse {
            issue_id: TICKET.to_string(),
            claimed: true,
            holder: Some(ASSIGNEE.to_string()),
        },
    );
    host.set(
        "POST",
        &contract_path(ServiceName::Tracker, "/releases"),
        &ReleaseResponse {
            issue_id: TICKET.to_string(),
            outcome: ReleaseOutcome::Released,
        },
    );
    host.set(
        "POST",
        &contract_path(ServiceName::Workspace, "/worktrees"),
        &ticket_worktree(),
    );
    host.set(
        "POST",
        &contract_path(ServiceName::AgentRunner, "/dispatches"),
        &DispatchResponse {
            attempt_id: ATTEMPT.to_string(),
            state: AttemptState::Running,
        },
    );
    host
}

// ---------------------------------------------------------------------------
// (a) env 装载
// ---------------------------------------------------------------------------

fn env_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn all_eight() -> HashMap<String, String> {
    env_map(&[
        (ENV_HOST, "127.0.0.1:3721"),
        (ENV_TOKEN, "op-token"),
        (ENV_ADMIN_TOKEN, "admin-token"),
        (ENV_ASSIGNEE, "orch-1"),
        (ENV_TICK_INTERVAL_MS, "250"),
        (ENV_REPO_PATH, "/repos/farm"),
        (ENV_WORKTREE_PATH, "/worktrees/{{ticket_id}}"),
        (ENV_WORKTREE_BRANCH, "farm/{{ticket_id}}"),
    ])
}

#[test]
fn the_environment_loads_into_the_config_and_the_layout_templates_expand_per_ticket() {
    let env = all_eight();
    let config = OrchestratorConfig::from_lookup(|name| env.get(name).cloned()).expect("loads");

    assert_eq!(config.host, "127.0.0.1:3721".parse().unwrap());
    assert_eq!(config.token, "op-token");
    assert_eq!(config.admin_token, "admin-token");
    assert_eq!(config.assignee, "orch-1");
    assert_eq!(config.tick_interval, Duration::from_millis(250));
    assert_eq!(
        config.layout.layout_for("T7"),
        WorktreeLayout {
            repo_path: "/repos/farm".to_string(),
            worktree_path: "/worktrees/T7".to_string(),
            worktree_branch: "farm/T7".to_string(),
        },
        "worktree 路径与分支是**模板**：每张票展开成自己的那一条（并发配额不是 1）"
    );
}

#[test]
fn every_required_variable_is_named_when_it_is_missing_and_there_is_no_default() {
    for missing in [
        ENV_HOST,
        ENV_TOKEN,
        ENV_ADMIN_TOKEN,
        ENV_ASSIGNEE,
        ENV_TICK_INTERVAL_MS,
        ENV_REPO_PATH,
        ENV_WORKTREE_PATH,
        ENV_WORKTREE_BRANCH,
    ] {
        let env = all_eight();
        let env: HashMap<String, String> =
            env.into_iter().filter(|(key, _)| key != missing).collect();
        let error = OrchestratorConfig::from_lookup(|name| env.get(name).cloned())
            .expect_err("must refuse to run half-configured");
        assert_eq!(error, ConfigError::Missing(missing));
        assert!(
            error.to_string().contains(missing),
            "错误消息必须指名变量：{error}"
        );
    }
}

#[test]
fn an_empty_variable_counts_as_missing() {
    let mut env = all_eight();
    env.insert(ENV_TOKEN.to_string(), String::new());
    let error = OrchestratorConfig::from_lookup(|name| env.get(name).cloned())
        .expect_err("empty is not a value");
    assert_eq!(error, ConfigError::Missing(ENV_TOKEN));
}

#[test]
fn a_malformed_host_or_a_non_positive_tick_interval_is_refused_by_name() {
    let mut env = all_eight();
    env.insert(ENV_HOST.to_string(), "not-an-address".to_string());
    assert!(matches!(
        OrchestratorConfig::from_lookup(|name| env.get(name).cloned()),
        Err(ConfigError::Invalid { var: ENV_HOST, .. })
    ));

    for bad in ["0", "-1", "soon"] {
        let mut env = all_eight();
        env.insert(ENV_TICK_INTERVAL_MS.to_string(), bad.to_string());
        assert!(
            matches!(
                OrchestratorConfig::from_lookup(|name| env.get(name).cloned()),
                Err(ConfigError::Invalid {
                    var: ENV_TICK_INTERVAL_MS,
                    ..
                })
            ),
            "{bad} 不是合法的 tick 间隔"
        );
    }
}

#[test]
fn the_injected_clock_has_the_host_timestamp_shape() {
    // 已知字面量：1970-01-01T00:00:00Z 与 2023-11-14T22:13:20Z（= 1_700_000_000）。
    assert_eq!(
        host_timestamp(std::time::UNIX_EPOCH),
        "1970-01-01T00:00:00Z"
    );
    assert_eq!(
        host_timestamp(std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
        "2023-11-14T22:13:20Z"
    );
    let shape = host_timestamp(SystemTime::now());
    assert_eq!(shape.len(), 20, "秒级形状（{shape}）");
    assert!(shape.ends_with('Z'), "UTC（{shape}）");
    assert_eq!(&shape[13..14], ":", "（{shape}）");
}

// ---------------------------------------------------------------------------
// (b) tick 序列
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_tick_reads_mappings_config_issues_and_the_event_increment_in_that_order() {
    let host = ready_host(
        config(Some(workflow()), Some(EffectiveSource::Fresh)),
        issues(None, IssueState::Open),
        empty_history(),
        Vec::new(),
    )
    .await;
    let client = ContractClient::new(host.addr, "op".to_string(), Some("admin".to_string()));
    let ledger = Mutex::new(EventLedger::default());
    let mut world = WorldModel::default();

    let report = tick_once(
        &client,
        &test_config(host.addr),
        &ledger,
        &mut world,
        "2026-01-01T00:00:00Z",
        1,
    )
    .await;

    assert_eq!(
        host.request_log(),
        vec![
            format!("GET {}", contract_path(ServiceName::Workspace, "/mappings")),
            format!(
                "GET {}",
                routes::admin_path(&format!("/workflow/config/{REPO}"))
            ),
            format!("GET {}", contract_path(ServiceName::Tracker, "/issues")),
            format!("GET {}", contract_path(ServiceName::Events, "/history")),
            format!("POST {}", contract_path(ServiceName::Tracker, "/claims")),
        ],
        "tick 序列：mappings（repo_id）→ 生效配置 → 视图 → 增量事件 → execute"
    );
    assert_eq!(
        report.decisions,
        vec![Decision::Claim {
            ticket_id: TICKET.to_string()
        }]
    );
    assert_eq!(
        report.outcomes,
        vec![TickOutcome::Claimed {
            ticket_id: TICKET.to_string()
        }]
    );
    assert_eq!(report.source, Some(EffectiveSource::Fresh));
}

#[tokio::test]
async fn the_next_tick_dispatches_the_ticket_i_hold_and_then_releases_the_merged_one() {
    let host = ready_host(
        config(Some(workflow()), Some(EffectiveSource::Fresh)),
        issues(None, IssueState::Open),
        empty_history(),
        Vec::new(),
    )
    .await;
    let client = ContractClient::new(host.addr, "op".to_string(), Some("admin".to_string()));
    let ledger = Mutex::new(EventLedger::default());
    let mut world = WorldModel::default();
    let config = test_config(host.addr);

    tick_once(
        &client,
        &config,
        &ledger,
        &mut world,
        "2026-01-01T00:00:00Z",
        1,
    )
    .await;

    // 视图翻到「在我名下、有活跃 attempt 之前的那个状态」：票被抢到了我手里。
    host.set(
        "GET",
        &contract_path(ServiceName::Tracker, "/issues"),
        &issues(Some(ASSIGNEE), IssueState::Claimed),
    );
    let report = tick_once(
        &client,
        &config,
        &ledger,
        &mut world,
        "2026-01-01T00:00:01Z",
        2,
    )
    .await;
    assert_eq!(
        report.decisions,
        vec![Decision::Dispatch {
            ticket_id: TICKET.to_string()
        }]
    );
    assert_eq!(
        report.outcomes,
        vec![TickOutcome::Dispatch {
            ticket_id: TICKET.to_string(),
            outcome: Outcome::Dispatched {
                attempt_id: ATTEMPT.to_string()
            },
        }],
        "派发序列：mappings → claim → revalidation → worktree → dispatch"
    );
    let counts = host.counts();
    assert_eq!(
        counts
            .get(&format!(
                "POST {}",
                contract_path(ServiceName::Workspace, "/worktrees")
            ))
            .copied(),
        Some(1)
    );
    assert_eq!(
        counts
            .get(&format!(
                "POST {}",
                contract_path(ServiceName::AgentRunner, "/dispatches")
            ))
            .copied(),
        Some(1),
        "本 tick 只派发一次；下一次由下一个 tick 的决策决定"
    );

    // merge 事件到账 + 视图落 done ⇒ 完成信号 ⇒ 交还排他所有权。
    host.set(
        "GET",
        &contract_path(ServiceName::Tracker, "/issues"),
        &issues(Some(ASSIGNEE), IssueState::Done),
    );
    host.set(
        "GET",
        &contract_path(ServiceName::Events, "/history"),
        &EventHistoryResponse {
            events: vec![merge_event()],
            truncated: false,
        },
    );
    ledger.lock().unwrap().append(merge_event());
    let report = tick_once(
        &client,
        &config,
        &ledger,
        &mut world,
        "2026-01-01T00:00:06Z",
        3,
    )
    .await;
    assert_eq!(
        report.decisions,
        vec![Decision::Release {
            ticket_id: TICKET.to_string()
        }]
    );
    assert_eq!(
        report.outcomes,
        vec![TickOutcome::Release {
            ticket_id: TICKET.to_string(),
            step: crate::exec::ReleaseStep::Released {
                outcome: ReleaseOutcome::Released
            },
        }]
    );
}

#[tokio::test]
async fn a_failed_dispatch_puts_the_ticket_in_cooldown_so_the_next_tick_does_not_redispatch_it() {
    // 票 #4 的「dispatch 非 service_unavailable 失败 ⇒ release + 记冷却」。这次失败在事件面
    // **没有任何事件**（宿主不会为一次没起来的 dispatch 发事件），所以冷却只能由 tick 自己
    // 折进世界；否则这张票每个 tick 都会被重新抢 + 重新派发，永远不停。
    let host = ready_host(
        config(Some(workflow()), Some(EffectiveSource::Fresh)),
        issues(Some(ASSIGNEE), IssueState::Claimed),
        empty_history(),
        Vec::new(),
    )
    .await;
    // 派发本身失败：`invalid_request`（只可能是我们自己编错了请求 ⇒ 放弃 + 交还 + 记冷却）。
    host.set_error(
        "POST",
        &contract_path(ServiceName::AgentRunner, "/dispatches"),
        ErrorCode::InvalidRequest,
    );
    let client = ContractClient::new(host.addr, "op".to_string(), Some("admin".to_string()));
    let ledger = Mutex::new(EventLedger::default());
    let mut world = WorldModel::default();
    let config = test_config(host.addr);

    let report = tick_once(
        &client,
        &config,
        &ledger,
        &mut world,
        "2026-01-01T00:00:00Z",
        1,
    )
    .await;
    assert!(
        matches!(
            report.outcomes.as_slice(),
            [TickOutcome::Dispatch {
                outcome: Outcome::Abandoned {
                    code: ErrorCode::InvalidRequest,
                    ..
                },
                ..
            }]
        ),
        "派发失败的分支：{:?}",
        report.outcomes
    );
    assert_eq!(
        world.backoff_until.get(TICKET).map(String::as_str),
        Some("2026-01-01T00:00:01Z"),
        "冷却截止时刻 = 失败时刻 + effective retry_backoff.base_delay_ms（1000ms）"
    );

    // 下一个 tick（同一秒里，tick 间隔 50ms）：冷却未过 ⇒ 不重派、也不重抢。
    let report = tick_once(
        &client,
        &config,
        &ledger,
        &mut world,
        "2026-01-01T00:00:00Z",
        2,
    )
    .await;
    assert!(
        report.decisions.is_empty(),
        "冷却中的票本 tick 不碰：{:?}",
        report.decisions
    );
    assert_eq!(
        dispatched_count(&host),
        1,
        "冷却期里没有第二次派发（这正是修之前每 tick 重派的那个洞）"
    );

    // 冷却过去之后（`now` 越过截止时刻）：重新派的——崩溃重启的续跑走的正是这条路。
    let report = tick_once(
        &client,
        &config,
        &ledger,
        &mut world,
        "2026-01-01T00:00:02Z",
        3,
    )
    .await;
    assert!(
        matches!(report.decisions.as_slice(), [Decision::Dispatch { .. }]),
        "冷却到期后重新派：{:?}",
        report.decisions
    );
    assert_eq!(dispatched_count(&host), 2);
}

/// 假宿主上收到过几次 dispatch 契约调用。
fn dispatched_count(host: &StubHost) -> usize {
    host.counts()
        .get(&format!(
            "POST {}",
            contract_path(ServiceName::AgentRunner, "/dispatches")
        ))
        .copied()
        .unwrap_or(0)
}

#[tokio::test]
async fn a_tick_that_cannot_pin_the_repo_id_stays_idle_and_does_not_invent_a_config() {
    // 两条映射并存、且都与我无关：没有任何契约字段能指出「本编排器的那条」。
    let host = StubHost::spawn(BTreeMap::new()).await;
    host.set(
        "GET",
        &contract_path(ServiceName::Workspace, "/mappings"),
        &MappingListResponse {
            mappings: vec![
                WorkspaceMapping {
                    spec_id: "spec-a".to_string(),
                    integration_branch: "feat/a".to_string(),
                    worktrees: Vec::new(),
                },
                WorkspaceMapping {
                    spec_id: "spec-b".to_string(),
                    integration_branch: "feat/b".to_string(),
                    worktrees: Vec::new(),
                },
            ],
        },
    );
    let client = ContractClient::new(host.addr, "op".to_string(), Some("admin".to_string()));
    let ledger = Mutex::new(EventLedger::default());
    let mut world = WorldModel::default();

    let report = tick_once(
        &client,
        &test_config(host.addr),
        &ledger,
        &mut world,
        "2026-01-01T00:00:00Z",
        1,
    )
    .await;

    assert_eq!(
        report,
        TickReport::default(),
        "推不出 repo_id ⇒ 本 tick idle"
    );
    assert_eq!(host.request_log().len(), 1, "只读了那一次 mappings");
}

// ---------------------------------------------------------------------------
// (c) WS 入账
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_newer_page_followed_by_an_older_one_still_shows_the_attempt_as_active() {
    // 事件账的两路来源（WS 快照 = 整个保留窗口；HTTP 增量页 = 最近一页）以**到达顺序**混进
    // 同一本账：宿主忙时/重启后，账上的到达顺序会是「较新的一页 ++ 较旧的一页」。按到达顺序
    // 折叠，一条比活跃 dispatch **更早**的终态事件会覆盖掉 active_attempts，于是 decide 会
    // 重新派发一张正在跑的票（`a_sigkill_then_a_restart_does_not_dispatch_twice` 守的正是这条
    // 不变量）。所以折叠用的是 `occurred_at`（契约唯一能给的顺序），不是到达顺序。
    let mut ledger = EventLedger::default();
    ledger.absorb(&EventHistoryResponse {
        events: vec![ticket_event(
            "e-newer",
            EventKind::Dispatch,
            "2026-01-01T00:00:10Z",
            "attempt-1",
        )],
        truncated: false,
    });
    ledger.absorb(&EventHistoryResponse {
        events: vec![ticket_event(
            "e-older",
            EventKind::Interrupted,
            "2026-01-01T00:00:09Z",
            "attempt-0",
        )],
        truncated: false,
    });

    let world = recon::rebuild(
        ASSIGNEE,
        &issues(Some(ASSIGNEE), IssueState::Claimed),
        &ledger.page(),
        &mappings(Vec::new()),
        None,
        &RetryBackoff::default(),
    );

    assert_eq!(
        world.active_attempts,
        BTreeMap::from([(TICKET.to_string(), Some("attempt-1".to_string()))]),
        "比 dispatch 更早的终态事件不得结束这个 attempt（否则会重派一张在跑的票）"
    );
    assert_eq!(
        world.event_watermark.as_deref(),
        Some("2026-01-01T00:00:10Z"),
        "游标 = 账上最新事件的 occurred_at，而不是最后到达的那一条"
    );
}

#[test]
fn the_event_ledger_is_append_only_dedupes_by_id_and_keeps_the_cursor_at_the_tail() {
    let mut ledger = EventLedger::default();
    assert!(ledger.is_empty());
    assert_eq!(ledger.watermark(), None);

    let first = merge_event();
    ledger.append(first.clone());
    ledger.append(first.clone()); // WS 快照与增量拉取会交叠：同一条事件只留一份。
    assert_eq!(ledger.len(), 1);

    let mut second = merge_event();
    second.id = "e-2".to_string();
    second.occurred_at = "2026-01-01T00:00:09Z".to_string();
    ledger.absorb(&EventHistoryResponse {
        events: vec![second, first],
        truncated: true,
    });

    assert_eq!(ledger.len(), 2);
    assert_eq!(
        ledger.watermark(),
        Some("2026-01-01T00:00:09Z"),
        "游标 = 最后一条已入账事件的 occurred_at"
    );
    assert!(ledger.page().truncated, "任一路读数说截断，账上就记着截断");
    assert_eq!(ledger.page().events.len(), 2);
}

#[test]
fn the_ws_hello_frame_carries_the_contract_version_and_the_subscribable_event_kinds() {
    let hello = WsHello::new("1.0.0", vec![EventKind::Dispatch, EventKind::Merge]);
    let text = serde_json::to_string(&hello).expect("hello serializes");
    let parsed: WsHello = serde_json::from_str(&text).expect("hello parses");
    assert_eq!(parsed, hello);
    assert_eq!(parsed.r#type, "hello", "hello 帧靠 `type` 与错误信封区分");
}

/// 假 WS 宿主：只讲 upgrade + 文本帧（服务端帧不带掩码）。
struct StubWs {
    addr: SocketAddr,
}

impl StubWs {
    async fn spawn(frames: Vec<Vec<u8>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub ws");
        let addr = listener.local_addr().expect("stub ws addr");
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let (_, path, _) = read_request(&mut socket).await;
            assert_eq!(
                path,
                contract_path(ServiceName::Events, "/stream"),
                "订阅打的是契约路由表里的那条路径"
            );
            let _ = socket
                .write_all(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n")
                .await;
            for frame in frames {
                let _ = socket.write_all(&frame).await;
            }
            let _ = socket.flush().await;
            let _ = socket.shutdown().await;
        });
        Self { addr }
    }
}

/// 一个服务端文本帧（FIN + opcode 0x1，不带掩码）。
fn text_frame(text: &str) -> Vec<u8> {
    let payload = text.as_bytes();
    let mut frame = vec![0x81u8];
    if payload.len() < 126 {
        frame.push(payload.len() as u8);
    } else {
        frame.push(126);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    }
    frame.extend_from_slice(payload);
    frame
}

#[tokio::test]
async fn the_stream_reads_hello_then_each_event_envelope_in_order() {
    let first = merge_event();
    let mut second = merge_event();
    second.id = "e-2".to_string();
    second.kind = EventKind::StallKill;
    second.occurred_at = "2026-01-01T00:00:09Z".to_string();
    let hello = WsHello::new(farm_sdk::CONTRACT_VERSION, vec![EventKind::Merge]);
    let ws = StubWs::spawn(vec![
        text_frame(&json(&hello)),
        text_frame(&json(&first)),
        text_frame(&json(&second)),
    ])
    .await;

    let mut stream = EventStream::connect(ws.addr, "op-token")
        .await
        .expect("upgrade accepted");
    assert_eq!(
        stream.hello().await.expect("hello").contract_version,
        farm_sdk::CONTRACT_VERSION
    );
    assert_eq!(stream.next_envelope().await.expect("frame"), Some(first));
    assert_eq!(stream.next_envelope().await.expect("frame"), Some(second));
    assert_eq!(
        stream.next_envelope().await.expect("eof"),
        None,
        "对端关闭 ⇒ 流结束"
    );
}

#[tokio::test]
async fn a_masked_frame_is_refused_instead_of_guessed() {
    // RFC 6455 §5.1：服务端帧不带掩码。带掩码的帧（不合规）不猜长度，直接报错。
    let masked = vec![0x81u8, 0x80, 0, 0, 0, 0];
    let ws = StubWs::spawn(vec![masked]).await;
    let mut stream = EventStream::connect(ws.addr, "op-token")
        .await
        .expect("upgrade");
    let error = stream
        .hello()
        .await
        .expect_err("must refuse a masked frame");
    assert!(error.to_string().contains("masked"), "{error}");
}

#[tokio::test]
async fn the_ws_task_only_books_events_and_never_touches_the_decision_path() {
    let first = merge_event();
    let hello = WsHello::new(farm_sdk::CONTRACT_VERSION, vec![EventKind::Merge]);
    let ws = StubWs::spawn(vec![text_frame(&json(&hello)), text_frame(&json(&first))]).await;

    let ledger = Mutex::new(EventLedger::default());
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut shutdown_rx = shutdown_rx;
    subscribe(ws.addr, "op-token", &ledger, &mut shutdown_rx)
        .await
        .expect("订阅正常收尾（对端关闭）");

    assert_eq!(ledger.lock().unwrap().len(), 1);
    assert_eq!(
        ledger.lock().unwrap().watermark(),
        Some("2026-01-01T00:00:05Z")
    );
    // 假 WS 宿主上没有任何契约 HTTP 路由：订阅任务一个决策 / 执行调用都没打过。
    // 决策的唯一路径是下一个 tick 的 reconcile（ADR-0001 决策 2）。
}

// ---------------------------------------------------------------------------
// (d) ADR-0004 三态
// ---------------------------------------------------------------------------

/// 观测 `alert` 字段的层（标注是验收的一部分）。
#[derive(Clone, Default)]
struct AlertLog(Arc<Mutex<Vec<String>>>);

#[derive(Default)]
struct AlertFields(String);

impl tracing::field::Visit for AlertFields {
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "alert" {
            self.0 = value.to_string();
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for AlertLog {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().level() == &tracing::Level::WARN {
            let mut fields = AlertFields::default();
            event.record(&mut fields);
            if !fields.0.is_empty() {
                self.0.lock().unwrap().push(fields.0);
            }
        }
    }
}

/// 在收集 `alert` 的 subscriber 下跑一个 tick（current_thread：订阅器是线程局部的，
/// 单线程运行时下整个 future 都在同一个线程上走）。
async fn tick_with_alerts(host: &StubHost, now: &str) -> (TickReport, Vec<String>) {
    let log = AlertLog::default();
    let subscriber = tracing_subscriber::registry().with(log.clone());
    let _guard = tracing::subscriber::set_default(subscriber);
    let client = ContractClient::new(host.addr, "op".to_string(), Some("admin".to_string()));
    let ledger = Mutex::new(EventLedger::default());
    let mut world = WorldModel::default();
    let report = tick_once(
        &client,
        &test_config(host.addr),
        &ledger,
        &mut world,
        now,
        1,
    )
    .await;
    let alerts = log.0.lock().unwrap().clone();
    (report, alerts)
}

#[tokio::test(flavor = "current_thread")]
async fn effective_none_is_idle_with_an_alert_and_no_invented_default_config() {
    let host = ready_host(
        config(None, None),
        issues(None, IssueState::Open),
        empty_history(),
        Vec::new(),
    )
    .await;

    let (report, alerts) = tick_with_alerts(&host, "2026-01-01T00:00:00Z").await;

    assert!(
        report.decisions.is_empty() && report.outcomes.is_empty(),
        "尚无生效配置 ⇒ idle（ADR-0004）"
    );
    assert_eq!(report.source, None);
    assert!(
        alerts.contains(&"workflow_config_refused".to_string()),
        "idle 必须告警：{alerts:?}"
    );
    let counts = host.counts();
    assert_eq!(
        counts.get(&format!(
            "POST {}",
            contract_path(ServiceName::Tracker, "/claims")
        )),
        None,
        "idle ⇒ 一张票也不抢"
    );
    assert_eq!(
        counts.get(&format!(
            "POST {}",
            contract_path(ServiceName::AgentRunner, "/dispatches")
        )),
        None,
        "idle ⇒ 一次也不派发"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_fresh_config_orchestrates_normally_and_is_not_annotated() {
    let host = ready_host(
        config(Some(workflow()), Some(EffectiveSource::Fresh)),
        issues(None, IssueState::Open),
        empty_history(),
        Vec::new(),
    )
    .await;

    let (report, alerts) = tick_with_alerts(&host, "2026-01-01T00:00:00Z").await;

    assert!(
        matches!(report.decisions.as_slice(), [Decision::Claim { .. }]),
        "fresh 配置照常编排：{:?}",
        report.decisions
    );
    assert!(
        !alerts.contains(&"workflow_config_last_known_good".to_string()),
        "fresh 不做 LKG 标注：{alerts:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_last_known_good_config_orchestrates_normally_and_is_annotated_every_tick() {
    let host = ready_host(
        config(Some(workflow()), Some(EffectiveSource::LastKnownGood)),
        issues(None, IssueState::Open),
        empty_history(),
        Vec::new(),
    )
    .await;

    let (report, alerts) = tick_with_alerts(&host, "2026-01-01T00:00:00Z").await;

    assert!(
        matches!(report.decisions.as_slice(), [Decision::Claim { .. }]),
        "LKG 照常编排（不是 idle）：{:?}",
        report.decisions
    );
    assert_eq!(report.source, Some(EffectiveSource::LastKnownGood));
    assert!(
        alerts.contains(&"workflow_config_last_known_good".to_string()),
        "LKG 必须持续标注：{alerts:?}"
    );
    assert_eq!(
        host.counts()
            .get(&format!(
                "POST {}",
                contract_path(ServiceName::Tracker, "/claims")
            ))
            .copied(),
        Some(1),
        "标注不妨碍编排：claim 照发"
    );
}

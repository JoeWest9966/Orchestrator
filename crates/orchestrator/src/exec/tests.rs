//! `exec` 的验收测试：派发序列（claim → revalidation → establish worktree → dispatch）、
//! 十码闭集分流、失败回滚（release + 记冷却）、worktree 布局推导、回收接线（交还是唯一动作）。
//!
//! 假宿主只讲契约的 HTTP 形状（状态行 + `Content-Length` + body + 关连接）：成功响应体由
//! farm-sdk 类型序列化，失败响应体由 [`ContractError`] 构造——期望值一律取夹具字面量或
//! 契约常量，不拿实现重算。

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use farm_sdk::services::agent_runner::{AttemptState, DispatchRequest, DispatchResponse};
use farm_sdk::services::tracker::{
    ClaimRequest, ClaimResponse, ReleaseOutcome, ReleaseRequest, ReleaseResponse,
};
use farm_sdk::services::workspace::{
    MappingListResponse, WorkspaceMapping, WorktreeEstablishRequest, WorktreeRef, WorktreeState,
};
use farm_sdk::workflow::{RetryBackoff, WorkflowConfig};
use farm_sdk::{contract_path, ContractError, ErrorCode, ServiceName};
use tracing_subscriber::layer::SubscriberExt;

use crate::contract::client::ContractClient;

use super::*;

// ---------------------------------------------------------------------------
// 夹具字面量（期望值的独立来源）
// ---------------------------------------------------------------------------

/// claim 身份（`FARM_ASSIGNEE`）。
const ME: &str = "orch-1";
/// 本票。
const TICKET: &str = "T7";
/// 另一张票（只出现在 mappings 里，用来证明复用判定按票走）。
const OTHER_TICKET: &str = "T9";
const SPEC: &str = "spec-10";
const INTEGRATION: &str = "implement/spec-10";
/// 派发 prompt 模板：只命名 `{{ticket_id}}`（票面唯一给出的占位符）。
const PROMPT_TEMPLATE: &str = "work on {{ticket_id}}";
/// effective `RetryBackoff` 的三个字段——冷却档位必须从这里来，不是本模块的常数。
const BASE_DELAY_MS: u64 = 7_000;
const BACKOFF_MULTIPLIER: u32 = 3;
const MAX_RETRIES: u32 = 5;

fn workflow() -> WorkflowConfig {
    WorkflowConfig {
        dispatch_labels: vec!["dispatch".to_string()],
        max_concurrency: 2,
        stall_threshold_secs: 3600,
        prompt_template: PROMPT_TEMPLATE.to_string(),
        retry_backoff: RetryBackoff {
            max_retries: MAX_RETRIES,
            base_delay_ms: BASE_DELAY_MS,
            backoff_multiplier: BACKOFF_MULTIPLIER,
        },
        projection: Default::default(),
    }
}

fn workflow_with_template(template: &str) -> WorkflowConfig {
    WorkflowConfig {
        prompt_template: template.to_string(),
        ..workflow()
    }
}

/// `[UNSOURCED]` 的三个路径字段（见模块头）。
fn layout() -> WorktreeLayout {
    WorktreeLayout {
        repo_path: "/repos/farm".to_string(),
        worktree_path: "/worktrees/t7".to_string(),
        worktree_branch: "farm/t7".to_string(),
    }
}

/// 一条 `(spec_id, integration_branch)` 映射、没有本票的 worktree：新建路径。
fn mappings_with_a_spec() -> MappingListResponse {
    MappingListResponse {
        mappings: vec![WorkspaceMapping {
            spec_id: SPEC.to_string(),
            integration_branch: INTEGRATION.to_string(),
            worktrees: Vec::new(),
        }],
    }
}

/// 本票已有 worktree 映射（外加一张别的票的 worktree）：复用路径。
fn mappings_with_a_worktree_for(ticket_id: &str, path: &str, branch: &str) -> MappingListResponse {
    MappingListResponse {
        mappings: vec![WorkspaceMapping {
            spec_id: SPEC.to_string(),
            integration_branch: INTEGRATION.to_string(),
            worktrees: vec![
                WorktreeRef {
                    ticket_id: OTHER_TICKET.to_string(),
                    path: "/worktrees/t9".to_string(),
                    branch: "farm/t9".to_string(),
                    state: WorktreeState::Ready,
                },
                WorktreeRef {
                    ticket_id: ticket_id.to_string(),
                    path: path.to_string(),
                    branch: branch.to_string(),
                    state: WorktreeState::Ready,
                },
            ],
        }],
    }
}

fn claimed(ticket_id: &str) -> ClaimResponse {
    ClaimResponse {
        issue_id: ticket_id.to_string(),
        claimed: true,
        holder: Some(ME.to_string()),
    }
}

fn worktree_ref() -> WorktreeRef {
    WorktreeRef {
        ticket_id: TICKET.to_string(),
        path: layout().worktree_path,
        branch: layout().worktree_branch,
        state: WorktreeState::Ready,
    }
}

fn dispatched(attempt_id: &str) -> DispatchResponse {
    DispatchResponse {
        attempt_id: attempt_id.to_string(),
        state: AttemptState::Running,
    }
}

/// 交还的结局投影：无人持有（幂等空操作）。
fn not_claimed() -> ReleaseResponse {
    ReleaseResponse {
        issue_id: TICKET.to_string(),
        outcome: ReleaseOutcome::NotClaimed,
    }
}

/// 契约路径（`farm-sdk` 的 `ServiceName` 是路径的服务段单一源，不手写字符串）。
fn path(service: ServiceName, suffix: &str) -> String {
    contract_path(service, suffix)
}

// ---------------------------------------------------------------------------
// 假宿主：按连接序吐预设响应，并记下收到的请求
// ---------------------------------------------------------------------------

/// 一份预设的 HTTP 响应。
struct Canned {
    status: u16,
    body: String,
}

impl Canned {
    fn to_bytes(&self) -> Vec<u8> {
        format!(
            "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            self.status,
            self.body.len(),
            self.body
        )
        .into_bytes()
    }
}

/// 2xx + 由 farm-sdk 类型序列化出的 body（wire 形状与契约一致）。
fn ok<T: serde::Serialize>(value: &T) -> Canned {
    Canned {
        status: 200,
        body: serde_json::to_string(value).expect("response serializes"),
    }
}

/// 闭集错误信封（`code` 的 HTTP 状态码 + `ContractError` 的 wire 形状）。
fn err(code: ErrorCode) -> Canned {
    Canned {
        status: code.http_status(),
        body: ContractError::new(code, "scripted failure").to_json(),
    }
}

/// 一条已收到的请求。
#[derive(Clone, Debug, Default)]
struct Sent {
    method: String,
    path: String,
    body: String,
}

/// 假宿主：顺序吐出预设响应。
struct FakeHost {
    addr: SocketAddr,
    sent: Arc<Mutex<Vec<Sent>>>,
    _task: tokio::task::JoinHandle<()>,
}

impl FakeHost {
    async fn spawn(canned: Vec<Canned>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake host");
        let addr = listener.local_addr().expect("fake host addr");
        let sent = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&sent);
        let task = tokio::spawn(async move {
            for response in canned {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let request = read_request(&mut socket).await;
                recorder.lock().expect("sent lock").push(request);
                let _ = socket.write_all(&response.to_bytes()).await;
                let _ = socket.flush().await;
                let _ = socket.shutdown().await;
            }
        });
        Self {
            addr,
            sent,
            _task: task,
        }
    }

    /// 操作面客户端（本序列不需要管理面）。
    fn client(&self) -> ContractClient {
        ContractClient::new(self.addr, "op-token", None)
    }

    fn sent(&self) -> Vec<Sent> {
        self.sent.lock().expect("sent lock").clone()
    }

    /// 打向某条路径的请求数。
    fn count_of(&self, path: &str) -> usize {
        self.sent().iter().filter(|sent| sent.path == path).count()
    }
}

/// 读一条完整的 HTTP/1.1 请求（请求行 + header + `Content-Length` 指定的 body）。
async fn read_request(socket: &mut TcpStream) -> Sent {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let read = socket.read(&mut chunk).await.unwrap_or(0);
        if read == 0 {
            return Sent::default();
        }
        buf.extend_from_slice(&chunk[..read]);
        let Some(head_end) = find_subslice(&buf, b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
        let mut lines = head.lines();
        let request_line = lines.next().unwrap_or_default().to_string();
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_string();
        let request_path = parts.next().unwrap_or_default().to_string();
        let content_length: usize = lines
            .filter_map(|line| line.split_once(": "))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.parse().ok())
            .unwrap_or(0);
        let mut body = buf[head_end + 4..].to_vec();
        while body.len() < content_length {
            let read = socket.read(&mut chunk).await.unwrap_or(0);
            if read == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..read]);
        }
        return Sent {
            method,
            path: request_path,
            body: String::from_utf8_lossy(&body).into_owned(),
        };
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn decode<T: serde::de::DeserializeOwned>(sent: &Sent) -> T {
    serde_json::from_str(&sent.body).expect("request body deserializes into the contract type")
}

/// 一次完整的成功序列要吐的响应：mappings → claim → claim → worktree → dispatch。
fn happy_path_responses(attempt_id: &str) -> Vec<Canned> {
    vec![
        ok(&mappings_with_a_spec()),
        ok(&claimed(TICKET)),
        ok(&claimed(TICKET)),
        ok(&worktree_ref()),
        ok(&dispatched(attempt_id)),
    ]
}

/// 在收集 WARN 的 subscriber 下跑一段异步代码，返回结果与收集到的 `(alert, code)`。
async fn capture_alerts<T>(
    body: impl std::future::Future<Output = T>,
) -> (T, Vec<(String, String)>) {
    crate::test_support::install_global_noop_subscriber();
    let log = WarningLog::default();
    let subscriber = tracing_subscriber::registry().with(log.clone());
    let guard = tracing::subscriber::set_default(subscriber);
    let value = body.await;
    drop(guard);
    let alerts = log.0.lock().expect("alert lock not poisoned").clone();
    (value, alerts)
}

/// 观测 `exec` 告警的层：每条 WARN 记下它的 `alert` 与 `code` 字段（告警可观测＝验收的一部分）。
#[derive(Clone, Default)]
struct WarningLog(Arc<Mutex<Vec<(String, String)>>>);

#[derive(Default)]
struct AlertFields {
    alert: String,
    code: String,
}

impl tracing::field::Visit for AlertFields {
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "alert" => self.alert = value.to_string(),
            "code" => self.code = value.to_string(),
            _ => {}
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarningLog {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().level() == &tracing::Level::WARN {
            let mut fields = AlertFields::default();
            event.record(&mut fields);
            self.0
                .lock()
                .expect("alert lock not poisoned")
                .push((fields.alert, fields.code));
        }
    }
}

// ---------------------------------------------------------------------------
// Slice 1：闭集分流
// ---------------------------------------------------------------------------

#[test]
fn every_closed_set_error_code_has_its_own_routing_branch() {
    let table = [
        (ErrorCode::Unauthorized, Disposition::Fatal),
        (ErrorCode::Forbidden, Disposition::Fatal),
        (ErrorCode::ContractVersionUnsupported, Disposition::Fatal),
        (ErrorCode::InvalidRequest, Disposition::AbandonedAndReleased),
        (ErrorCode::NotFound, Disposition::AbandonedNotMine),
        (ErrorCode::MethodNotAllowed, Disposition::Fatal),
        (ErrorCode::Conflict, Disposition::AbandonedNotMine),
        (ErrorCode::NotImplemented, Disposition::Fatal),
        (ErrorCode::ServiceUnavailable, Disposition::Retry),
        (ErrorCode::Internal, Disposition::Fatal),
    ];
    assert_eq!(table.len(), ErrorCode::ALL.len());

    for (code, expected) in table {
        assert_eq!(route(code), expected, "{code} 的分流");
    }
}

// ---------------------------------------------------------------------------
// Slice 2：派发序列
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_sequence_claims_revalidates_builds_the_worktree_and_dispatches() {
    let host = FakeHost::spawn(happy_path_responses("attempt-1")).await;
    let client = host.client();

    let outcome = dispatch_ticket(&client, ME, TICKET, &workflow(), &layout()).await;

    assert_eq!(
        outcome,
        Outcome::Dispatched {
            attempt_id: "attempt-1".to_string()
        }
    );

    // 契约调用序列：读 mappings（只读）→ claim → revalidation → 建 worktree → 派发。
    let sent = host.sent();
    assert_eq!(
        sent.iter()
            .map(|s| (s.method.as_str(), s.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("GET", path(ServiceName::Workspace, "/mappings").as_str()),
            ("POST", path(ServiceName::Tracker, "/claims").as_str()),
            ("POST", path(ServiceName::Tracker, "/claims").as_str()),
            ("POST", path(ServiceName::Workspace, "/worktrees").as_str()),
            (
                "POST",
                path(ServiceName::AgentRunner, "/dispatches").as_str()
            ),
        ]
    );

    // 两次同形 claim：claim + 幂等 revalidation（同 assignee）。
    assert_eq!(
        decode::<ClaimRequest>(&sent[1]),
        ClaimRequest {
            issue_id: TICKET.to_string(),
            assignee: ME.to_string(),
        }
    );
    assert_eq!(
        decode::<ClaimRequest>(&sent[2]),
        decode::<ClaimRequest>(&sent[1])
    );

    // 建 worktree：`spec_id` / `integration_branch` 来自 mappings，三条 `[UNSOURCED]` 路径来自 layout。
    assert_eq!(
        decode::<WorktreeEstablishRequest>(&sent[3]),
        WorktreeEstablishRequest {
            spec_id: SPEC.to_string(),
            integration_branch: INTEGRATION.to_string(),
            ticket_id: TICKET.to_string(),
            repo_path: layout().repo_path,
            worktree_path: layout().worktree_path,
            worktree_branch: layout().worktree_branch,
            start_point: None,
        }
    );

    // 派发：`prompt_template` 展开 `{{ticket_id}}`。
    assert_eq!(
        decode::<DispatchRequest>(&sent[4]),
        DispatchRequest {
            issue_id: TICKET.to_string(),
            profile: None,
            prompt: Some("work on T7".to_string()),
        }
    );
}

#[tokio::test]
async fn only_the_ticket_id_placeholder_is_expanded() {
    let host = FakeHost::spawn(happy_path_responses("attempt-2")).await;
    let client = host.client();

    let workflow = workflow_with_template("work on {{ticket_id}} of {{spec_id}}");
    let outcome = dispatch_ticket(&client, ME, TICKET, &workflow, &layout()).await;
    assert_eq!(
        outcome,
        Outcome::Dispatched {
            attempt_id: "attempt-2".to_string()
        }
    );

    let request: DispatchRequest = decode(&host.sent()[4]);
    assert_eq!(
        request.prompt,
        Some("work on T7 of {{spec_id}}".to_string()),
        "票面只命名了 {{{{ticket_id}}}}；别的占位符是 [UNSOURCED]，不猜、不展开"
    );
}

#[tokio::test]
async fn a_ticket_whose_worktree_is_already_mapped_reuses_the_mapped_path_and_branch() {
    let host = FakeHost::spawn(vec![
        ok(&mappings_with_a_worktree_for(
            TICKET,
            "/worktrees/t7-mapped",
            "farm/t7-mapped",
        )),
        ok(&claimed(TICKET)),
        ok(&claimed(TICKET)),
        ok(&WorktreeRef {
            ticket_id: TICKET.to_string(),
            path: "/worktrees/t7-mapped".to_string(),
            branch: "farm/t7-mapped".to_string(),
            state: WorktreeState::Ready,
        }),
        ok(&dispatched("attempt-3")),
    ])
    .await;

    let outcome = dispatch_ticket(&host.client(), ME, TICKET, &workflow(), &layout()).await;
    assert_eq!(
        outcome,
        Outcome::Dispatched {
            attempt_id: "attempt-3".to_string()
        }
    );

    // 复用判定按票走：用映射里的 path/branch，而不是 layout 的（`repo_path` 仍然只能来自 layout）。
    let request: WorktreeEstablishRequest = decode(&host.sent()[3]);
    assert_eq!(request.worktree_path, "/worktrees/t7-mapped");
    assert_eq!(request.worktree_branch, "farm/t7-mapped");
    assert_eq!(request.repo_path, layout().repo_path);
    assert_eq!(request.spec_id, SPEC);
    assert_eq!(request.integration_branch, INTEGRATION);
}

#[tokio::test]
async fn an_underivable_worktree_layout_claims_nothing_and_emits_no_worktree_or_dispatch() {
    let host = FakeHost::spawn(vec![ok(&MappingListResponse {
        mappings: Vec::new(),
    })])
    .await;
    let client = host.client();

    let (outcome, alerts) =
        capture_alerts(dispatch_ticket(&client, ME, TICKET, &workflow(), &layout())).await;

    assert_eq!(outcome, Outcome::LayoutUnderivable);
    assert_eq!(
        host.sent().len(),
        1,
        "推不出建 worktree 的请求就不抢票：只读了 mappings 这一条只读快照"
    );
    assert_eq!(alerts.len(), 1, "推不出就告警一次");
}

// ---------------------------------------------------------------------------
// Slice 3：十码在派发序列里的分支
// ---------------------------------------------------------------------------

/// 一张票在派发步失败时，序列的处置（测试里逐码断言）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Branch {
    /// 重试：不 release、不冷却。
    Retried,
    /// 放弃：票不在我名下，不 release、照记冷却、不告警。
    AbandonedNotMine,
    /// 放弃 + release + 冷却 + 告警（我方 bug）。
    AbandonedAndReleased,
    /// 硬失败 + release + 冷却 + 告警。
    HardFailure,
}

#[tokio::test]
async fn every_closed_set_error_code_takes_its_own_branch_in_the_sequence() {
    let table = [
        (ErrorCode::Unauthorized, Branch::HardFailure),
        (ErrorCode::Forbidden, Branch::HardFailure),
        (ErrorCode::ContractVersionUnsupported, Branch::HardFailure),
        (ErrorCode::InvalidRequest, Branch::AbandonedAndReleased),
        (ErrorCode::NotFound, Branch::AbandonedNotMine),
        (ErrorCode::MethodNotAllowed, Branch::HardFailure),
        (ErrorCode::Conflict, Branch::AbandonedNotMine),
        (ErrorCode::NotImplemented, Branch::HardFailure),
        (ErrorCode::ServiceUnavailable, Branch::Retried),
        (ErrorCode::Internal, Branch::HardFailure),
    ];
    assert_eq!(table.len(), ErrorCode::ALL.len());

    for (code, branch) in table {
        // 失败注入在派发步（spec §5.3 的「dispatch 失败 → release + 记冷却」）。
        let host = FakeHost::spawn(vec![
            ok(&mappings_with_a_spec()),
            ok(&claimed(TICKET)),
            ok(&claimed(TICKET)),
            ok(&worktree_ref()),
            err(code),
            ok(&not_claimed()),
        ])
        .await;
        let client = host.client();

        let (outcome, alerts) =
            capture_alerts(dispatch_ticket(&client, ME, TICKET, &workflow(), &layout())).await;

        let releases = host.count_of(&path(ServiceName::Tracker, "/releases"));
        match branch {
            Branch::Retried => {
                assert_eq!(outcome, Outcome::Retried { code }, "{code}");
                assert_eq!(releases, 0, "{code}：重试不 release");
                assert!(alerts.is_empty(), "{code}：重试不告警");
            }
            Branch::AbandonedNotMine => {
                assert_eq!(
                    outcome,
                    Outcome::Abandoned {
                        code,
                        rollback: Rollback {
                            release: None,
                            cooldown: Cooldown {
                                ticket_id: TICKET.to_string(),
                                delay_ms: BASE_DELAY_MS,
                            },
                        },
                    },
                    "{code}"
                );
                assert_eq!(releases, 0, "{code}：票不在我名下，交还无从谈起");
                assert!(alerts.is_empty(), "{code}：不告警");
            }
            Branch::AbandonedAndReleased | Branch::HardFailure => {
                let expected = if branch == Branch::HardFailure {
                    Outcome::HardFailure {
                        code,
                        rollback: Rollback {
                            release: Some(Ok(ReleaseOutcome::NotClaimed)),
                            cooldown: Cooldown {
                                ticket_id: TICKET.to_string(),
                                delay_ms: BASE_DELAY_MS,
                            },
                        },
                    }
                } else {
                    Outcome::Abandoned {
                        code,
                        rollback: Rollback {
                            release: Some(Ok(ReleaseOutcome::NotClaimed)),
                            cooldown: Cooldown {
                                ticket_id: TICKET.to_string(),
                                delay_ms: BASE_DELAY_MS,
                            },
                        },
                    }
                };
                assert_eq!(outcome, expected, "{code}");
                assert_eq!(releases, 1, "{code}：回滚交还一次");
                assert_eq!(
                    decode::<ReleaseRequest>(
                        host.sent()
                            .iter()
                            .find(|sent| sent.path == path(ServiceName::Tracker, "/releases"))
                            .expect("交还请求已发出")
                    ),
                    ReleaseRequest {
                        issue_id: TICKET.to_string(),
                        assignee: ME.to_string(),
                    },
                    "{code}：交还的是本票与本 assignee"
                );
                assert_eq!(alerts.len(), 1, "{code}：告警一次");
                assert_eq!(alerts[0].1, code.as_str(), "{code}：告警带上闭集码");
            }
        }
    }
}

#[tokio::test]
async fn a_conflicting_claim_abandons_the_ticket_before_any_worktree_or_dispatch() {
    let host = FakeHost::spawn(vec![ok(&mappings_with_a_spec()), err(ErrorCode::Conflict)]).await;

    let outcome = dispatch_ticket(&host.client(), ME, TICKET, &workflow(), &layout()).await;

    assert_eq!(
        outcome,
        Outcome::Abandoned {
            code: ErrorCode::Conflict,
            rollback: Rollback {
                release: None,
                cooldown: Cooldown {
                    ticket_id: TICKET.to_string(),
                    delay_ms: BASE_DELAY_MS,
                },
            },
        }
    );
    assert_eq!(host.sent().len(), 2, "冲突后不再建 worktree、不再派发");
    assert_eq!(
        host.count_of(&path(ServiceName::Workspace, "/worktrees")),
        0
    );
    assert_eq!(
        host.count_of(&path(ServiceName::AgentRunner, "/dispatches")),
        0
    );
}

#[tokio::test]
async fn a_claim_response_that_says_not_claimed_is_the_conflict_semantics() {
    // `ClaimResponse::claimed == false` 的契约注释：false = 已被他人持有（调用方收到 conflict 语义）。
    let host = FakeHost::spawn(vec![
        ok(&mappings_with_a_spec()),
        ok(&ClaimResponse {
            issue_id: TICKET.to_string(),
            claimed: false,
            holder: Some("orch-2".to_string()),
        }),
    ])
    .await;

    let outcome = dispatch_ticket(&host.client(), ME, TICKET, &workflow(), &layout()).await;

    assert_eq!(
        outcome,
        Outcome::Abandoned {
            code: ErrorCode::Conflict,
            rollback: Rollback {
                release: None,
                cooldown: Cooldown {
                    ticket_id: TICKET.to_string(),
                    delay_ms: BASE_DELAY_MS,
                },
            },
        }
    );
    assert_eq!(host.sent().len(), 2);
}

#[tokio::test]
async fn a_service_unavailable_dispatch_is_retried_without_release_and_without_cooldown() {
    let host = FakeHost::spawn(vec![
        ok(&mappings_with_a_spec()),
        ok(&claimed(TICKET)),
        ok(&claimed(TICKET)),
        ok(&worktree_ref()),
        err(ErrorCode::ServiceUnavailable),
    ])
    .await;

    let outcome = dispatch_ticket(&host.client(), ME, TICKET, &workflow(), &layout()).await;

    assert_eq!(
        outcome,
        Outcome::Retried {
            code: ErrorCode::ServiceUnavailable
        },
        "唯一 is_retryable() 为真的码：不放弃本票"
    );
    assert_eq!(
        host.count_of(&path(ServiceName::Tracker, "/releases")),
        0,
        "重试语义不 release——退避后再来的是同一张票"
    );
}

#[tokio::test]
async fn the_cooldown_delay_comes_from_the_effective_retry_backoff() {
    let host = FakeHost::spawn(vec![
        ok(&mappings_with_a_spec()),
        ok(&claimed(TICKET)),
        ok(&claimed(TICKET)),
        ok(&worktree_ref()),
        err(ErrorCode::Internal),
        ok(&not_claimed()),
    ])
    .await;

    // 换一份 effective 配置：冷却必须跟着配置走，不是本模块的常数。
    let mut workflow = workflow();
    workflow.retry_backoff = RetryBackoff {
        max_retries: 2,
        base_delay_ms: 1_234,
        backoff_multiplier: 9,
    };

    let outcome = dispatch_ticket(&host.client(), ME, TICKET, &workflow, &layout()).await;

    let Outcome::HardFailure { rollback, .. } = outcome else {
        panic!("internal 是硬失败分支，得到 {outcome:?}");
    };
    assert_eq!(rollback.cooldown.delay_ms, 1_234);
    assert_eq!(rollback.cooldown.ticket_id, TICKET);
}

// ---------------------------------------------------------------------------
// Slice 4：回收接线——交还是唯一动作
// ---------------------------------------------------------------------------

#[tokio::test]
async fn releasing_a_ticket_is_a_single_hand_back_and_the_idempotent_outcome_is_not_an_error() {
    let host = FakeHost::spawn(vec![ok(&ReleaseResponse {
        issue_id: TICKET.to_string(),
        outcome: ReleaseOutcome::Released,
    })])
    .await;

    let step = release_ticket(&host.client(), ME, TICKET, &workflow()).await;

    assert_eq!(
        step,
        ReleaseStep::Released {
            outcome: ReleaseOutcome::Released
        }
    );
    let sent = host.sent();
    assert_eq!(sent.len(), 1, "交还只打一条契约路由");
    assert_eq!(sent[0].method, "POST");
    assert_eq!(sent[0].path, path(ServiceName::Tracker, "/releases"));
    assert_eq!(
        decode::<ReleaseRequest>(&sent[0]),
        ReleaseRequest {
            issue_id: TICKET.to_string(),
            assignee: ME.to_string(),
        }
    );
}

#[tokio::test]
async fn the_not_claimed_release_outcome_is_an_idempotent_no_op() {
    let host = FakeHost::spawn(vec![ok(&not_claimed())]).await;

    let step = release_ticket(&host.client(), ME, TICKET, &workflow()).await;

    assert_eq!(
        step,
        ReleaseStep::Released {
            outcome: ReleaseOutcome::NotClaimed
        },
        "无人持有是幂等空操作，不是错误"
    );
}

#[tokio::test]
async fn a_retryable_release_failure_is_retried_and_a_hard_one_cools_down() {
    let retryable = FakeHost::spawn(vec![err(ErrorCode::ServiceUnavailable)]).await;
    assert_eq!(
        release_ticket(&retryable.client(), ME, TICKET, &workflow()).await,
        ReleaseStep::Retried {
            code: ErrorCode::ServiceUnavailable
        }
    );

    let fatal = FakeHost::spawn(vec![err(ErrorCode::Unauthorized)]).await;
    let (step, alerts) =
        capture_alerts(release_ticket(&fatal.client(), ME, TICKET, &workflow())).await;
    assert_eq!(
        step,
        ReleaseStep::Failed {
            code: ErrorCode::Unauthorized,
            cooldown: Cooldown {
                ticket_id: TICKET.to_string(),
                delay_ms: BASE_DELAY_MS,
            },
        }
    );
    assert_eq!(alerts.len(), 1);
}

// ---------------------------------------------------------------------------
// Slice 5：纪律（源码层的机械化证明）
// ---------------------------------------------------------------------------

#[test]
fn the_exec_module_folds_no_events_runs_no_projection_and_shells_out_to_nothing() {
    // 事件的折叠是 recon 的单一源；投影是宿主的机械动作；git / worktree 的建立是 workspace
    // 服务的活。执行器只发契约调用——这些 token 一个都不该出现在实现里。
    let source = include_str!("mod.rs");
    for forbidden in [
        "EventKind",          // 不重新解释事件（recon 折叠，exec 不重复）
        "projection",         // 投影零参与
        "ProjectionSwitches", // 同上（连配置字段都不读）
        "close_issue",
        "open_pr",
        "add_label",
        "comment_merge_commit",
        "MergeProposal", // 不代替 agent 提 merge
        "std::process",  // 不自己 shell out（git worktree add 是宿主的活）
        "Command",
        "std::fs",
        "std::env", // 布局不从 env 猜（[UNSOURCED]）
    ] {
        assert!(
            !source.contains(forbidden),
            "exec 实现里不得出现 {forbidden}"
        );
    }
}

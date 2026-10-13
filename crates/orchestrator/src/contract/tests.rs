//! [`ContractClient`] 的契约面单测：内存内 TCP 假宿主 + 每个 typed 方法的成败断言。
//!
//! 假宿主只讲契约的 HTTP 形状（状态行 + `Content-Length` + body + 关闭连接），响应体
//! 由 farm-sdk 类型序列化或契约 wire 夹具给出；期望值一律取契约常量/夹具字面量，
//! 不拿实现重算。

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use farm_sdk::events::EventKind;
use farm_sdk::handshake::{ClientInfo, ClientKind, HandshakeRequest, HandshakeResponse};
use farm_sdk::services::agent_runner::{AttemptState, DispatchRequest, DispatchResponse};
use farm_sdk::services::events::EventHistoryQuery;
use farm_sdk::services::tracker::{
    ClaimRequest, ClaimResponse, IssueState, ReleaseOutcome, ReleaseRequest, ReleaseResponse,
};
use farm_sdk::services::workspace::{WorktreeEstablishRequest, WorktreeRef, WorktreeState};
use farm_sdk::workflow::{
    EffectiveSource, ProjectionSwitches, RetryBackoff, WorkflowConfig, WorkflowConfigStatus,
};
use farm_sdk::{ContractError, ErrorCode, ServiceName};

use super::client::ContractClient;

// ---------------------------------------------------------------------------
// 内存内假宿主
// ---------------------------------------------------------------------------

/// 一份预设的 HTTP 响应。
#[derive(Clone, Debug)]
struct CannedResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl CannedResponse {
    fn json(status: u16, body: &str) -> Self {
        Self {
            status,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: body.to_string(),
        }
    }

    fn text(status: u16, body: &str) -> Self {
        Self {
            status,
            headers: vec![("Content-Type".to_string(), "text/plain".to_string())],
            body: body.to_string(),
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut out = format!("HTTP/1.1 {} OK\r\n", self.status);
        for (name, value) in &self.headers {
            out.push_str(&format!("{name}: {value}\r\n"));
        }
        out.push_str(&format!("Content-Length: {}\r\n", self.body.len()));
        out.push_str("Connection: close\r\n\r\n");
        out.push_str(&self.body);
        out.into_bytes()
    }
}

/// 由 farm-sdk 类型序列化出的 200 响应体（保证 wire 形状与契约一致）。
fn json_ok<T: serde::Serialize>(value: &T) -> CannedResponse {
    CannedResponse::json(
        200,
        &serde_json::to_string(value).expect("response serializes"),
    )
}

/// 一条已收到的请求（供断言客户端真的按契约发请求）。
#[derive(Clone, Debug, Default)]
struct CapturedRequest {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: String,
}

impl CapturedRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// 假宿主：按连接序返回 `responses`，并把每个收到的请求记进 `requests`。
struct FakeHost {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    _task: tokio::task::JoinHandle<()>,
}

impl FakeHost {
    async fn spawn(responses: Vec<CannedResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake host");
        let addr = listener.local_addr().expect("fake host addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            for response in responses {
                let (mut socket, _) = match listener.accept().await {
                    Ok(accepted) => accepted,
                    Err(_) => return,
                };
                let request = read_request(&mut socket).await;
                recorder.lock().expect("requests lock").push(request);
                let _ = socket.write_all(&response.to_bytes()).await;
                let _ = socket.flush().await;
                let _ = socket.shutdown().await;
            }
        });
        Self {
            addr,
            requests,
            _task: task,
        }
    }

    /// 操作面客户端（无 admin token）。
    fn client(&self, op_token: &str) -> ContractClient {
        ContractClient::new(self.addr, op_token, None)
    }

    /// 操作面 + 管理面 token 齐备的客户端。
    fn client_with_admin(&self, op_token: &str, admin_token: &str) -> ContractClient {
        ContractClient::new(self.addr, op_token, Some(admin_token.to_string()))
    }

    fn requests(&self) -> Vec<CapturedRequest> {
        self.requests.lock().expect("requests lock").clone()
    }

    fn request_count(&self) -> usize {
        self.requests.lock().expect("requests lock").len()
    }
}

/// 读一条完整的 HTTP/1.1 请求（请求行 + header + `Content-Length` 指定的 body）。
async fn read_request(socket: &mut TcpStream) -> CapturedRequest {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let read = socket.read(&mut chunk).await.unwrap_or(0);
        if read == 0 {
            return CapturedRequest::default();
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
        let path = parts.next().unwrap_or_default().to_string();
        let headers: Vec<(String, String)> = lines
            .filter_map(|line| {
                line.split_once(": ")
                    .map(|(key, value)| (key.to_ascii_lowercase(), value.to_string()))
            })
            .collect();
        let content_length: usize = headers
            .iter()
            .find(|(key, _)| key == "content-length")
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
        return CapturedRequest {
            method,
            path,
            headers,
            body: String::from_utf8_lossy(&body).into_owned(),
        };
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// ---------------------------------------------------------------------------
// Slice 1：握手
// ---------------------------------------------------------------------------

#[tokio::test]
async fn handshake_sends_contract_version_constant_and_parses_handshake_response() {
    let host = FakeHost::spawn(vec![json_ok(&HandshakeResponse::for_host("0.1.0"))]).await;
    let client = host.client("op-token");

    let handshake = client.handshake().await.expect("handshake succeeds");

    // 期望值来自契约常量（独立源），不是拿实现重算。
    assert_eq!(handshake.contract_version, farm_sdk::CONTRACT_VERSION);
    assert_eq!(handshake.host_version, "0.1.0");
    assert_eq!(handshake.services, ServiceName::ALL.to_vec());

    // 客户端发出的握手请求必须走 farm-sdk 类型，且声明契约版本常量。
    let captured = host.requests();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].method, "POST");
    assert_eq!(captured[0].path, farm_sdk::HANDSHAKE_PATH);
    let sent: HandshakeRequest =
        serde_json::from_str(&captured[0].body).expect("typed handshake request");
    assert_eq!(sent.contract_version, farm_sdk::CONTRACT_VERSION);
    assert_eq!(sent.client.kind, ClientKind::Orchestrator);
    assert_eq!(
        sent.client,
        ClientInfo {
            name: "orchestrator".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            kind: ClientKind::Orchestrator
        }
    );
}

// ---------------------------------------------------------------------------
// Slice 1b：契约主版本被钉住（50-canonical-cargo.md §4.2）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_contract_major_is_pinned_to_one() {
    // 编译期常量取自 farm-sdk；期望值 1 是契约的主版本号本身，不拿实现重算。
    assert_eq!(farm_sdk::CONTRACT_MAJOR, 1);

    let host = FakeHost::spawn(vec![json_ok(&HandshakeResponse::for_host("0.1.0"))]).await;
    let client = host.client("op-token");

    let handshake = client.handshake().await.expect("handshake succeeds");

    // 运行时核对：宿主报的契约版本就是钉住的那一个，且它的主版本是 1。
    assert_eq!(handshake.contract_version, farm_sdk::CONTRACT_VERSION);
    assert_eq!(handshake.contract_version.split('.').next(), Some("1"));
}

// ---------------------------------------------------------------------------
// Slice 2：tracker 路由（issues / claims / releases）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn issues_parses_wire_issue_list_fixture() {
    // 逐字节的契约 wire 夹具（源：farm-sdk `IssueListResponse` 的 wire 形状）。
    let body = r#"{"issues":[{"issue_id":"42","title":"wire fixture","state":"open","labels":["ready"],"assignee":"me","blocked_by":["41"]}]}"#;
    let host = FakeHost::spawn(vec![CannedResponse::json(200, body)]).await;
    let client = host.client("op-token");

    let list = client.issues().await.expect("issues succeeds");

    assert_eq!(list.issues.len(), 1);
    assert_eq!(list.issues[0].issue_id, "42");
    assert_eq!(list.issues[0].state, IssueState::Open);
    assert_eq!(list.issues[0].labels, vec!["ready".to_string()]);
    assert_eq!(list.issues[0].assignee.as_deref(), Some("me"));
    assert_eq!(list.issues[0].blocked_by, vec!["41".to_string()]);

    let captured = host.requests();
    assert_eq!(captured[0].method, "GET");
    assert_eq!(captured[0].path, "/v1/tracker/issues");
    assert_eq!(captured[0].header("x-farm-token"), Some("op-token"));
}

#[tokio::test]
async fn claim_posts_typed_request_and_parses_claim_response() {
    let response = ClaimResponse {
        issue_id: "7".to_string(),
        claimed: true,
        holder: Some("me".to_string()),
    };
    let host = FakeHost::spawn(vec![json_ok(&response)]).await;
    let client = host.client("op-token");

    let got = client.claim("7", "me").await.expect("claim succeeds");

    assert_eq!(got, response);
    let captured = host.requests();
    assert_eq!(captured[0].method, "POST");
    assert_eq!(captured[0].path, "/v1/tracker/claims");
    let sent: ClaimRequest = serde_json::from_str(&captured[0].body).expect("typed claim request");
    assert_eq!(
        sent,
        ClaimRequest {
            issue_id: "7".to_string(),
            assignee: "me".to_string()
        }
    );
}

#[tokio::test]
async fn release_posts_typed_request_and_parses_release_response() {
    let response = ReleaseResponse {
        issue_id: "7".to_string(),
        outcome: ReleaseOutcome::Released,
    };
    let host = FakeHost::spawn(vec![json_ok(&response)]).await;
    let client = host.client("op-token");

    let got = client.release("7", "me").await.expect("release succeeds");

    assert_eq!(got, response);
    let captured = host.requests();
    assert_eq!(captured[0].method, "POST");
    assert_eq!(captured[0].path, "/v1/tracker/releases");
    let sent: ReleaseRequest =
        serde_json::from_str(&captured[0].body).expect("typed release request");
    assert_eq!(
        sent,
        ReleaseRequest {
            issue_id: "7".to_string(),
            assignee: "me".to_string()
        }
    );
}

#[tokio::test]
async fn release_projects_the_idempotent_not_claimed_outcome() {
    let response = ReleaseResponse {
        issue_id: "7".to_string(),
        outcome: ReleaseOutcome::NotClaimed,
    };
    let host = FakeHost::spawn(vec![json_ok(&response)]).await;
    let client = host.client("op-token");

    let got = client.release("7", "me").await.expect("release succeeds");

    assert_eq!(got.outcome, ReleaseOutcome::NotClaimed);
}

// ---------------------------------------------------------------------------
// Slice 3：agent-runner / workspace
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatch_posts_attempt_start_request_and_parses_dispatch_response() {
    let response = DispatchResponse {
        attempt_id: "a-1".to_string(),
        state: AttemptState::Running,
    };
    let host = FakeHost::spawn(vec![json_ok(&response)]).await;
    let client = host.client("op-token");

    let req = DispatchRequest {
        issue_id: "t-1".to_string(),
        profile: Some("orchestrator".to_string()),
        prompt: None,
    };
    let got = client.dispatch(&req).await.expect("dispatch succeeds");

    assert_eq!(got, response);
    let captured = host.requests();
    assert_eq!(captured[0].method, "POST");
    assert_eq!(captured[0].path, "/v1/agent-runner/dispatches");
    let sent: DispatchRequest =
        serde_json::from_str(&captured[0].body).expect("typed dispatch request");
    assert_eq!(sent, req);
}

#[tokio::test]
async fn establish_worktree_posts_typed_request_and_parses_worktree_ref() {
    let response = WorktreeRef {
        ticket_id: "t-1".to_string(),
        path: "/tmp/a-1".to_string(),
        branch: "feat/t1".to_string(),
        state: WorktreeState::Ready,
    };
    let host = FakeHost::spawn(vec![json_ok(&response)]).await;
    let client = host.client("op-token");

    let req = WorktreeEstablishRequest {
        spec_id: "s-1".to_string(),
        integration_branch: "main".to_string(),
        ticket_id: "t-1".to_string(),
        repo_path: "/repo".to_string(),
        worktree_path: "/tmp/a-1".to_string(),
        worktree_branch: "feat/t1".to_string(),
        start_point: Some("main".to_string()),
    };
    let got = client
        .establish_worktree(&req)
        .await
        .expect("worktree establish succeeds");

    assert_eq!(got, response);
    let captured = host.requests();
    assert_eq!(captured[0].method, "POST");
    assert_eq!(captured[0].path, "/v1/workspace/worktrees");
    let sent: WorktreeEstablishRequest =
        serde_json::from_str(&captured[0].body).expect("typed worktree request");
    assert_eq!(sent, req);
}

#[tokio::test]
async fn mappings_lists_workspace_mappings_fixture() {
    let body = r#"{"mappings":[{"spec_id":"s-1","integration_branch":"main","worktrees":[]}]}"#;
    let host = FakeHost::spawn(vec![CannedResponse::json(200, body)]).await;
    let client = host.client("op-token");

    let list = client.mappings().await.expect("mappings succeeds");

    assert_eq!(list.mappings.len(), 1);
    assert_eq!(list.mappings[0].spec_id, "s-1");
    assert_eq!(list.mappings[0].integration_branch, "main");
    let captured = host.requests();
    assert_eq!(captured[0].method, "GET");
    assert_eq!(captured[0].path, "/v1/workspace/mappings");
}

// ---------------------------------------------------------------------------
// Slice 4：事件历史 / 宿主管理面 / 闭集错误分流
// ---------------------------------------------------------------------------

/// 把请求路径的查询串解码成键值对（测试侧自带的百分号解码；不借实现的编码器）。
fn decode_query(path: &str) -> Vec<(String, String)> {
    let Some((_, query)) = path.split_once('?') else {
        return Vec::new();
    };
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (key, value) = part.split_once('=').expect("query pair has '='");
            (percent_decode(key), percent_decode(value))
        })
        .collect();
    pairs.sort();
    pairs
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).expect("hex digits are ascii");
            out.push(u8::from_str_radix(hex, 16).expect("valid percent escape"));
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).expect("decoded query is utf8")
}

#[tokio::test]
async fn event_history_sends_every_query_field_and_parses_the_envelope_fixture() {
    let body = r#"{"events":[{"id":"e-1","kind":"merge","occurred_at":"2024-01-02T03:04:06Z","actor":"host","ticket_id":"t-1","audited":true}],"truncated":true}"#;
    let host = FakeHost::spawn(vec![CannedResponse::json(200, body)]).await;
    let client = host.client("op-token");

    let query = EventHistoryQuery {
        since: Some("2024-01-02T03:04:05Z".to_string()),
        kind: Some(EventKind::Merge),
        ticket_id: Some("t-1".to_string()),
        limit: Some(7),
    };
    let history = client
        .event_history(&query)
        .await
        .expect("event history succeeds");

    assert_eq!(history.events.len(), 1);
    assert_eq!(history.events[0].id, "e-1");
    assert_eq!(history.events[0].kind, EventKind::Merge);
    assert_eq!(history.events[0].ticket_id.as_deref(), Some("t-1"));
    assert!(history.truncated);

    let captured = host.requests();
    assert_eq!(captured[0].method, "GET");
    assert!(
        captured[0].path.starts_with("/v1/events/history?"),
        "path was {}",
        captured[0].path
    );
    // 期望值取契约 wire 字面量（`EventKind::Merge` 的 wire 形 = "merge"）。
    assert_eq!(
        decode_query(&captured[0].path),
        vec![
            ("kind".to_string(), "merge".to_string()),
            ("limit".to_string(), "7".to_string()),
            ("since".to_string(), "2024-01-02T03:04:05Z".to_string()),
            ("ticket_id".to_string(), "t-1".to_string()),
        ]
    );
}

#[tokio::test]
async fn event_history_omits_the_query_separator_when_no_filter_is_set() {
    let body = r#"{"events":[],"truncated":false}"#;
    let host = FakeHost::spawn(vec![CannedResponse::json(200, body)]).await;
    let client = host.client("op-token");

    client
        .event_history(&EventHistoryQuery::default())
        .await
        .expect("event history succeeds");

    let captured = host.requests();
    assert_eq!(captured[0].path, "/v1/events/history");
}

#[tokio::test]
async fn workflow_config_reads_the_admin_route_with_the_admin_token() {
    let status = WorkflowConfigStatus {
        repo_id: "r1".to_string(),
        effective: Some(WorkflowConfig {
            dispatch_labels: vec!["ready-for-agent".to_string()],
            max_concurrency: 3,
            stall_threshold_secs: 1800,
            prompt_template: "do the ticket".to_string(),
            retry_backoff: RetryBackoff::default(),
            projection: ProjectionSwitches::default(),
        }),
        source: Some(EffectiveSource::Fresh),
        loaded_at: Some("2024-01-02T03:04:05Z".to_string()),
        last_known_good: None,
        last_alert: None,
    };
    let host = FakeHost::spawn(vec![json_ok(&status)]).await;
    let client = host.client_with_admin("op-token", "admin-token");

    let got = client
        .workflow_config("r1")
        .await
        .expect("workflow config succeeds");

    assert_eq!(got, status);
    let captured = host.requests();
    assert_eq!(captured[0].method, "GET");
    assert_eq!(captured[0].path, "/admin/workflow/config/r1");
    assert_eq!(
        captured[0].header("x-farm-admin-token"),
        Some("admin-token")
    );
    // 管理面不得混用操作面凭据（ADR-0002 §1）。
    assert_eq!(captured[0].header("x-farm-token"), None);
}

#[tokio::test]
async fn admin_route_is_refused_locally_when_no_admin_token_is_configured() {
    let host = FakeHost::spawn(vec![]).await;
    let client = host.client("op-token");

    let err = client
        .workflow_config("r1")
        .await
        .expect_err("admin route is refused");

    assert_eq!(err.code, ErrorCode::Unauthorized);
    // 拒绝必须是客户端的：不得把请求发出去。
    assert_eq!(host.request_count(), 0);
}

#[tokio::test]
async fn every_closed_set_error_code_maps_to_its_own_variant_and_http_status() {
    for code in ErrorCode::ALL {
        let envelope = ContractError::new(code, format!("host says {}", code.as_str()))
            .with_details(serde_json::json!({ "holder": "someone" }));
        let host = FakeHost::spawn(vec![CannedResponse::json(
            code.http_status(),
            &envelope.to_json(),
        )])
        .await;
        let client = host.client("op-token");

        let err = client.issues().await.expect_err("non-2xx is an error");

        assert_eq!(
            err.code,
            code,
            "wire code {} must survive the round trip",
            code.as_str()
        );
        assert_eq!(err.message, format!("host says {}", code.as_str()));
        assert_eq!(
            err.details
                .as_ref()
                .and_then(|d| d.get("holder"))
                .and_then(|h| h.as_str()),
            Some("someone")
        );
        assert_eq!(err.retryable, code.is_retryable());
    }
}

#[tokio::test]
async fn a_non_envelope_error_body_becomes_a_closed_set_internal() {
    let host = FakeHost::spawn(vec![CannedResponse::text(502, "<html>bad gateway</html>")]).await;
    let client = host.client("op-token");

    let err = client
        .issues()
        .await
        .expect_err("non-envelope body is an error");

    assert_eq!(err.code, ErrorCode::Internal);
}

#[tokio::test]
async fn an_unparseable_2xx_body_becomes_a_closed_set_internal() {
    let host = FakeHost::spawn(vec![CannedResponse::json(
        200,
        "{\"issues\":\"not a list\"}",
    )])
    .await;
    let client = host.client("op-token");

    let err = client
        .issues()
        .await
        .expect_err("unparseable 2xx is an error");

    assert_eq!(err.code, ErrorCode::Internal);
}

#[tokio::test]
async fn an_unreachable_host_is_a_retryable_service_unavailable() {
    // 绑定后立即释放：拿到一个几乎肯定没人监听的地址。
    let addr = {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind probe");
        listener.local_addr().expect("probe addr")
    };
    let client = ContractClient::new(addr, "op-token", None);

    let err = client
        .issues()
        .await
        .expect_err("unreachable host is an error");

    assert_eq!(err.code, ErrorCode::ServiceUnavailable);
    assert!(err.retryable);
}

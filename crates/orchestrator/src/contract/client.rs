//! 契约客户端 [`ContractClient`]。
//!
//! 演化自 Farm 骨架 `examples/orchestrator-skeleton/src/client.rs`：以契约客户端身份
//! 驱动宿主（只过 farm-sdk 类型 + 契约 HTTP API，无私有后门）。契约面用 op token，
//! 宿主管理面用**独立** admin token（管理 token ≠ 操作 token）。每条 typed 方法对应
//! 一条契约路由，响应一律解析成 farm-sdk 类型，错误一律落闭集 [`ContractError`]。

use std::net::SocketAddr;

use farm_sdk::handshake::{
    AuthScheme, ClientInfo, ClientKind, HandshakeRequest, HandshakeResponse, CONTRACT_VERSION,
    HANDSHAKE_PATH,
};
use farm_sdk::routes::ADMIN_ROUTES;
use farm_sdk::services::agent_runner::{DispatchRequest, DispatchResponse};
use farm_sdk::services::events::{EventHistoryQuery, EventHistoryResponse};
use farm_sdk::services::tracker::{
    ClaimRequest, ClaimResponse, IssueListResponse, ReleaseRequest, ReleaseResponse,
};
use farm_sdk::services::workspace::{MappingListResponse, WorktreeEstablishRequest, WorktreeRef};
use farm_sdk::workflow::WorkflowConfigStatus;
use farm_sdk::{contract_path, ContractError, ServiceName};

use super::http::{http_request, io_err};

/// 以契约客户端身份驱动宿主。
///
/// 契约面请求带 op token；宿主管理面请求带独立 admin token——`admin_token` 为 `None`
/// （管理面未配置/禁用）时，管理面 typed 方法**直接拒绝、不发任何请求**。
#[derive(Clone, Debug)]
pub struct ContractClient {
    pub addr: SocketAddr,
    pub op_token: String,
    pub admin_token: Option<String>,
}

impl ContractClient {
    pub fn new(addr: SocketAddr, op_token: impl Into<String>, admin_token: Option<String>) -> Self {
        Self {
            addr,
            op_token: op_token.into(),
            admin_token,
        }
    }

    /// 契约面请求（op token）。header 名取自 farm-sdk 的 `AuthScheme`，不写死字符串。
    pub async fn contract(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> Result<(u16, String), ContractError> {
        let auth = AuthScheme::default();
        http_request(
            self.addr,
            method,
            path,
            Some((auth.http_header.as_str(), self.op_token.as_str())),
            body,
        )
        .await
        .map_err(io_err)
    }

    /// 宿主管理面请求（独立 admin token）。
    ///
    /// `admin_token` 为 `None`（管理面未配置/禁用）时**直接拒绝、不发请求**：闭集
    /// `unauthorized`，不把凭据缺失伪装成连接错误。header 名同样取自 `AuthScheme`。
    pub async fn admin(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> Result<(u16, String), ContractError> {
        let Some(token) = &self.admin_token else {
            return Err(ContractError::admin_unauthorized(
                "the host admin API is disabled: no admin token is configured",
            ));
        };
        let auth = AuthScheme::default();
        http_request(
            self.addr,
            method,
            path,
            Some((auth.admin_header.as_str(), token.as_str())),
            body,
        )
        .await
        .map_err(io_err)
    }

    /// 把一次契约请求的结果解析成 `T`（2xx）或闭集 [`ContractError`]（其余）。
    ///
    /// 这是「错误码单一源」的实现位：非 2xx 的 body 就是契约错误信封，解析不了也落成
    /// `internal`，绝不静默成功。
    fn parse<T: serde::de::DeserializeOwned>(status: u16, body: &str) -> Result<T, ContractError> {
        if (200..300).contains(&status) {
            serde_json::from_str(body).map_err(|e| {
                ContractError::internal(format!(
                    "host returned HTTP {status} but an unparseable body: {e}: {body}"
                ))
            })
        } else {
            Err(
                serde_json::from_str::<ContractError>(body).unwrap_or_else(|e| {
                    ContractError::internal(format!(
                        "host returned HTTP {status} with a non-envelope body: {e}: {body}"
                    ))
                }),
            )
        }
    }

    /// 握手：声明本客户端实现的契约版本与身份。
    pub async fn handshake(&self) -> Result<HandshakeResponse, ContractError> {
        let request = HandshakeRequest {
            contract_version: CONTRACT_VERSION.to_string(),
            client: ClientInfo {
                name: "orchestrator".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                kind: ClientKind::Orchestrator,
            },
        };
        let body = serde_json::to_string(&request).expect("HandshakeRequest serializes");
        let (status, body) = self.contract("POST", HANDSHAKE_PATH, Some(&body)).await?;
        Self::parse(status, &body)
    }

    /// 读全量归一化票视图（`GET /v1/tracker/issues`）。
    pub async fn issues(&self) -> Result<IssueListResponse, ContractError> {
        let path = contract_path(ServiceName::Tracker, "/issues");
        let (status, body) = self.contract("GET", &path, None).await?;
        Self::parse(status, &body)
    }

    /// 原子抢票（`POST /v1/tracker/claims`）；被他人持有即闭集 `conflict`。
    pub async fn claim(
        &self,
        issue_id: &str,
        assignee: &str,
    ) -> Result<ClaimResponse, ContractError> {
        let request = ClaimRequest {
            issue_id: issue_id.to_string(),
            assignee: assignee.to_string(),
        };
        let body = serde_json::to_string(&request).expect("ClaimRequest serializes");
        let path = contract_path(ServiceName::Tracker, "/claims");
        let (status, body) = self.contract("POST", &path, Some(&body)).await?;
        Self::parse(status, &body)
    }

    /// 交还排他所有权（`POST /v1/tracker/releases`）；无人持有是幂等空操作。
    pub async fn release(
        &self,
        issue_id: &str,
        assignee: &str,
    ) -> Result<ReleaseResponse, ContractError> {
        let request = ReleaseRequest {
            issue_id: issue_id.to_string(),
            assignee: assignee.to_string(),
        };
        let body = serde_json::to_string(&request).expect("ReleaseRequest serializes");
        let path = contract_path(ServiceName::Tracker, "/releases");
        let (status, body) = self.contract("POST", &path, Some(&body)).await?;
        Self::parse(status, &body)
    }

    /// 派发 agent（`POST /v1/agent-runner/dispatches`）；`profile` 可选。
    pub async fn dispatch(
        &self,
        request: &DispatchRequest,
    ) -> Result<DispatchResponse, ContractError> {
        let body = serde_json::to_string(request).expect("DispatchRequest serializes");
        let path = contract_path(ServiceName::AgentRunner, "/dispatches");
        let (status, body) = self.contract("POST", &path, Some(&body)).await?;
        Self::parse(status, &body)
    }

    /// 委托建立 ticket worktree（`POST /v1/workspace/worktrees`）。
    pub async fn establish_worktree(
        &self,
        request: &WorktreeEstablishRequest,
    ) -> Result<WorktreeRef, ContractError> {
        let body = serde_json::to_string(request).expect("WorktreeEstablishRequest serializes");
        let path = contract_path(ServiceName::Workspace, "/worktrees");
        let (status, body) = self.contract("POST", &path, Some(&body)).await?;
        Self::parse(status, &body)
    }

    /// 读 spec ↔ 集成分支 ↔ worktrees 映射（`GET /v1/workspace/mappings`）。
    pub async fn mappings(&self) -> Result<MappingListResponse, ContractError> {
        let path = contract_path(ServiceName::Workspace, "/mappings");
        let (status, body) = self.contract("GET", &path, None).await?;
        Self::parse(status, &body)
    }

    /// 查询事件历史（`GET /v1/events/history`）。
    ///
    /// 查询字段取自 [`EventHistoryQuery`] 本身（serde 序列化出发键名 + 百分号编码），
    /// 不手写 wire 名；空查询不带 `?`。
    pub async fn event_history(
        &self,
        query: &EventHistoryQuery,
    ) -> Result<EventHistoryResponse, ContractError> {
        let suffix = encode_query(query)?;
        let path = format!(
            "{}{}",
            contract_path(ServiceName::Events, "/history"),
            suffix
        );
        let (status, body) = self.contract("GET", &path, None).await?;
        Self::parse(status, &body)
    }

    /// 查询某被编排仓库当前生效的 workflow 配置与 LKG 状态
    /// （`GET /admin/workflow/config/{repo_id}`，走 admin token）。
    pub async fn workflow_config(
        &self,
        repo_id: &str,
    ) -> Result<WorkflowConfigStatus, ContractError> {
        let path = admin_repo_path(repo_id);
        let (status, body) = self.admin("GET", &path, None).await?;
        Self::parse(status, &body)
    }
}

/// 管理面上「按 `repo_id` 查配置」那条路由的实路径。
///
/// 路径由 [`ADMIN_ROUTES`]（路由表的单一源，`farm_sdk::routes` 头：「改路径 = 改契约，
/// 必须三处同步」）渲染，不在这里手写。表里用 `{repo_id}` 占位；找不到唯一一条含它的
/// workflow 配置路由就 panic——那是契约形状变了，不是调用方该猜的。
///
/// 查找键只取 `"/workflow/config/"` 这一小段（用来在表里认出那条路由），完整路径仍然
/// 来自表本身：改路径只需改表，这里会跟着变。
pub(super) fn admin_repo_path(repo_id: &str) -> String {
    const KEY: &str = "/workflow/config/";
    let templates: Vec<&str> = ADMIN_ROUTES
        .iter()
        .map(|route| route.path)
        .filter(|path| path.contains(KEY))
        .collect();
    let [template] = templates.as_slice() else {
        panic!(
            "ADMIN_ROUTES must declare exactly one route containing {KEY:?}, found {}",
            templates.len()
        );
    };
    assert!(
        template.contains("{repo_id}"),
        "the workflow config route template must carry a {{repo_id}} placeholder: {template}"
    );
    // 表里的 path 已是完整路径（含 `/admin` 前缀）——直接替换占位符，不再拼一次前缀。
    template.replacen("{repo_id}", &percent_encode(repo_id), 1)
}

/// 把 [`EventHistoryQuery`] 编成查询串（含前导 `?`；无字段时为空串）。
///
/// 键名由 serde 序列化出（契约字段名单一源）；值按 RFC 3986 百分号编码。
fn encode_query(query: &EventHistoryQuery) -> Result<String, ContractError> {
    let serde_json::Value::Object(fields) = serde_json::to_value(query)
        .map_err(|e| ContractError::internal(format!("EventHistoryQuery is unencodable: {e}")))?
    else {
        return Err(ContractError::internal(
            "EventHistoryQuery is not a JSON object",
        ));
    };
    let pairs: Vec<String> = fields
        .iter()
        .map(|(key, value)| {
            let raw = match value {
                serde_json::Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            format!("{}={}", percent_encode(key), percent_encode(&raw))
        })
        .collect();
    if pairs.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!("?{}", pairs.join("&")))
    }
}

/// RFC 3986 百分号编码：保留 unreserved 字符，其余逐字节 `%XX`（大写十六进制）。
fn percent_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(char::from(byte));
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

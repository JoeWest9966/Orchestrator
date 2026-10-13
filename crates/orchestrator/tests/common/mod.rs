//! 集成测试夹具：拉起**真实 farm-host 进程** + fake-pi（复刻 skeleton 的 harness 形状，
//! `/var/opt/Farm-ref/examples/orchestrator-skeleton/src/harness.rs` 与 `tests/common/mod.rs`）。
//!
//! 二进制定位优先级：`*_BIN` 环境变量 > `$CARGO_TARGET_DIR/debug/` > manifest 相对路径。
//! 本仓的 `CARGO_TARGET_DIR` 是 worktree 本地的 target，宿主与替身都不在那里，所以跑 e2e 时
//! **显式**给 `FARM_HOST_BIN` 与 `FAKE_PI_BIN`（`60-environment-and-risks.md` R5：不显式指定
//! 就会静默 skip）。
//!
//! 编排器**只**需要宿主与 agent 替身：宿主自己按 env 装配替身，编排器不感知 agent 跑在哪
//! （ADR-0001 决策 4 / ADR-0006），也不发通知（通知是宿主的活）。
//!
//! 这个文件被**每个**集成测试目标各编一次（各目标自己 `mod common;`），帮手按目标取用：
//! 对 `dispatch_sequence` 来说，主循环 e2e 专用的那几个当然「未使用」。`dead_code` 的前提
//! 在这里不成立（它们都被 `tests/main_loop_e2e.rs` 用着），故整模块关掉这条噪声。
#![allow(dead_code)]

use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use orchestrator::contract::client::ContractClient;
use tokio::net::TcpStream;

/// 播种票的标签：夹具里唯一的 dispatchable 标签（与测试侧 `.farm/workflow.json` 的
/// `dispatch_labels` 必须一致）。
pub const SEED_LABEL: &str = "dispatch";

/// 定位 `farm-host` 二进制。
pub fn host_bin() -> Option<PathBuf> {
    bin_from("FARM_HOST_BIN", "farm-host")
}

/// 定位 `fake-pi` 替身二进制。
pub fn fake_pi_bin() -> Option<PathBuf> {
    bin_from("FAKE_PI_BIN", "fake-pi")
}

/// `*_BIN` > `$CARGO_TARGET_DIR/debug/<name>`（skeleton 的优先序）。
fn bin_from(env_var: &str, name: &str) -> Option<PathBuf> {
    if let Ok(path) = std::env::var(env_var) {
        return Some(PathBuf::from(path));
    }
    let dir = std::env::var("CARGO_TARGET_DIR").ok()?;
    let candidate = PathBuf::from(dir).join("debug").join(name);
    candidate.exists().then_some(candidate)
}

/// 取一个临时回环端口（预绑 `:0` 拿 OS 分配的端口后放开，交给宿主 bind）。
pub fn ephemeral_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr
}

async fn wait_listening(addr: SocketAddr, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if TcpStream::connect(addr).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// 一个被测的真实 farm-host 进程 + 它的临时目录（Drop 时一起收尾）。
pub struct TestHost {
    child: Child,
    /// 宿主的回环地址：测试要拿它当 `FARM_HOST` 喂给编排器、并按契约直接打宿主。
    pub addr: SocketAddr,
    op_token: String,
    admin_token: String,
    _dir: tempfile::TempDir,
}

impl TestHost {
    /// spawn 宿主：临时回环端口、tempdir 里的库、播种若干 open 原生票（`dispatch` 标签），
    /// **带** admin token（读生效配置要它：`60-environment-and-risks.md` R6）。
    pub async fn spawn(ticket_numbers: &[u32], extra_env: &[(&str, String)]) -> Self {
        let bin = host_bin().expect("farm-host 二进制（调用方先查过存在性）");
        let dir = tempfile::tempdir().expect("tempdir");
        let addr = ephemeral_port();
        let op_token = "e2e-op-token".to_string();
        let admin_token = "e2e-admin-token".to_string();

        let tickets: Vec<String> = ticket_numbers
            .iter()
            .map(|number| {
                format!(
                    r#"{{"number":{number},"title":"ticket {number}","state":"open","labels":["{SEED_LABEL}"],"blocked_by":[]}}"#
                )
            })
            .collect();
        let tickets_path = dir.path().join("tickets.json");
        std::fs::write(&tickets_path, format!("[{}]", tickets.join(","))).expect("write tickets");

        let mut command = Command::new(&bin);
        command
            .env("FARM_BIND", addr.to_string())
            .env("FARM_TOKEN", &op_token)
            .env("FARM_ADMIN_TOKEN", &admin_token)
            .env("FARM_DB", dir.path().join("farm.db"))
            .env("FARM_NATIVE_TICKETS", &tickets_path)
            .env("RUST_LOG", "warn"); // 安静：断言不靠日志
        for (key, value) in extra_env {
            command.env(key, value);
        }

        let child = command
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {}: {e}", bin.display()));
        let host = Self {
            child,
            addr,
            op_token,
            admin_token,
            _dir: dir,
        };
        assert!(
            wait_listening(addr, Duration::from_secs(30)).await,
            "farm-host 没有在 {addr} 上监听"
        );
        host
    }

    /// 契约面 + 管理面 token 齐备的客户端。
    pub fn client(&self) -> ContractClient {
        ContractClient::new(
            self.addr,
            self.op_token.clone(),
            Some(self.admin_token.clone()),
        )
    }
}

impl Drop for TestHost {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// 机械断言的报告簿（skeleton `report.rs` 形状的极简版）：e2e 每一步记一条，最后既断言
/// 「全绿」也断言「条数够」——后者是防空洞：半途而废不能让测试变绿。
#[derive(Default)]
pub struct Checks {
    entries: Vec<(String, bool, String)>,
}

impl Checks {
    pub fn record(&mut self, name: &str, ok: bool, detail: String) {
        self.entries.push((name.to_string(), ok, detail));
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn all_ok(&self) -> bool {
        self.entries.iter().all(|(_, ok, _)| *ok)
    }

    pub fn render(&self) -> String {
        self.entries
            .iter()
            .map(|(name, ok, detail)| {
                format!(
                    "[{}] {name}\n      {detail}",
                    if *ok { "ok  " } else { "FAIL" }
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

// ---------------------------------------------------------------------------
// 编排器进程夹具（ticket #5 的 e2e：真宿主 + **真编排器二进制** + fake-pi）
// ---------------------------------------------------------------------------

/// 定位被测的编排器二进制。
///
/// Cargo 为同包集成测试注入 `CARGO_BIN_EXE_<name>`——它**总是**存在（`cargo test` 会先编
/// 二进制），所以这一条不会静默 skip。其余二进制走 `*_BIN` / `CARGO_TARGET_DIR`（骨架的
/// 优先序），找不到就由调用方跳过（`60-environment-and-risks.md` R5）。
pub fn orchestrator_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_orchestrator"))
}

/// 编排器子进程的 env（票 #5 的闭集五 + worktree 三条 `[UNSOURCED]` 扩展，见 `main_loop`）。
pub struct OrchestratorEnv {
    pub host: SocketAddr,
    pub token: String,
    pub admin_token: String,
    pub assignee: String,
    pub tick_interval_ms: u64,
    pub repo_path: String,
    pub worktree_path: String,
    pub worktree_branch: String,
}

impl OrchestratorEnv {
    /// 一组 env（worktree 路径 / 分支是**模板**：每张票展开成自己的那条）。
    pub fn new(host: SocketAddr, repo_path: &str, worktrees_root: &std::path::Path) -> Self {
        Self {
            host,
            token: "e2e-op-token".to_string(),
            admin_token: "e2e-admin-token".to_string(),
            assignee: "orchestrator-e2e".to_string(),
            tick_interval_ms: 50,
            repo_path: repo_path.to_string(),
            worktree_path: worktrees_root
                .join("{{ticket_id}}")
                .to_string_lossy()
                .into_owned(),
            worktree_branch: "farm/{{ticket_id}}".to_string(),
        }
    }
}

/// 一个被测的**真编排器进程**：日志落文件（断言 LKG 标注 / tick 活性），可被 `SIGKILL`。
pub struct OrchestratorRun {
    child: Child,
    log_path: PathBuf,
}

impl OrchestratorRun {
    /// spawn 编排器：日志（stdout + stderr）落 `log_path`，级别 `debug`（tick 行也要）。
    pub fn spawn(env: &OrchestratorEnv, log_path: PathBuf) -> Self {
        let bin = orchestrator_bin();
        let log = std::fs::File::create(&log_path).expect("create orchestrator log");
        let child = Command::new(&bin)
            .env("FARM_HOST", env.host.to_string())
            .env("FARM_TOKEN", &env.token)
            .env("FARM_ADMIN_TOKEN", &env.admin_token)
            .env("FARM_ASSIGNEE", &env.assignee)
            .env("TICK_INTERVAL_MS", env.tick_interval_ms.to_string())
            .env("FARM_REPO_PATH", &env.repo_path)
            .env("FARM_WORKTREE_PATH", &env.worktree_path)
            .env("FARM_WORKTREE_BRANCH", &env.worktree_branch)
            .env("RUST_LOG", "debug")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::from(
                log.try_clone().expect("clone log"),
            ))
            .stderr(std::process::Stdio::from(log))
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {}: {e}", bin.display()));
        Self { child, log_path }
    }

    /// `SIGKILL`（`Child::kill` 在 Unix 上就是 `kill -9`）：模拟编排器被硬杀。
    pub fn sigkill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// 进程还在跑吗（`Ok(None)` = 还在）。
    pub fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// 到目前为止的日志全文（可能还没有内容：进程刚起）。
    pub fn log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }
}

impl Drop for OrchestratorRun {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// 测试侧以自己的身份打契约（**扮演 agent**，不是编排器的能力）
// ---------------------------------------------------------------------------

/// 测试扮演 agent 提交 merge 提议：`POST /v1/merge/proposals`。
///
/// 这条路由**不在编排器的范围里**（提议是 agent 的活，`farm-host/src/merge.rs`：agent 只能
/// 提议、宿主机械校验后执行）。所以它不进 `ContractClient`——测试自己用契约类型 + 契约 HTTP
/// 机器打，零手写 wire 名（路径来自 `farm_sdk::contract_path`，token header 来自 `AuthScheme`）。
pub async fn propose_merge(
    addr: SocketAddr,
    token: &str,
    proposal: &farm_sdk::services::merge::MergeProposal,
) -> Result<farm_sdk::services::merge::MergeProposalResponse, String> {
    use farm_sdk::handshake::AuthScheme;
    use farm_sdk::{contract_path, ServiceName};
    let body = serde_json::to_string(proposal).map_err(|e| e.to_string())?;
    let auth = AuthScheme::default();
    let (status, text) = orchestrator::contract::http::http_request(
        addr,
        "POST",
        &contract_path(ServiceName::Merge, "/proposals"),
        Some((auth.http_header.as_str(), token)),
        Some(&body),
    )
    .await
    .map_err(|e| e.to_string())?;
    if !(200..300).contains(&status) {
        return Err(format!("HTTP {status}: {text}"));
    }
    serde_json::from_str(&text).map_err(|e| format!("{e}: {text}"))
}

// ---------------------------------------------------------------------------
// 轮询（用**截止时刻**，不用固定 sleep：`60-environment-and-risks.md` R11）
// ---------------------------------------------------------------------------

/// 反复探测直到有值或超时。探测闭包里的 await 借的是闭包外的作用域（不会借闭包本身）。
pub async fn poll<T, Fut, F>(timeout: Duration, mut probe: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = probe().await {
            return Some(value);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 事件历史里某张票的某种事件（观测一律走契约面的事件账）。
pub async fn events_of_kind(
    client: &ContractClient,
    ticket_id: &str,
    kind: farm_sdk::events::EventKind,
) -> Vec<farm_sdk::events::EventEnvelope> {
    match client
        .event_history(&farm_sdk::services::events::EventHistoryQuery {
            ticket_id: Some(ticket_id.to_string()),
            ..Default::default()
        })
        .await
    {
        Ok(history) => history
            .events
            .into_iter()
            .filter(|event| event.kind == kind)
            .collect(),
        Err(_) => Vec::new(),
    }
}

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
    addr: SocketAddr,
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

//! Orchestrator 二进制入口（ticket #5 / spec §7）。
//!
//! 入口只做三件事：装载 env 配置 → 装日志 → 把 `SIGINT` / `SIGTERM` 变成 `main_loop`
//! 的停机信号。循环本身（tick 装配 + WS 入账任务）在 [`orchestrator::main_loop`]。
//!
//! env 是**闭集五**（`FARM_HOST` / `FARM_TOKEN` / `FARM_ADMIN_TOKEN` / `FARM_ASSIGNEE` /
//! `TICK_INTERVAL_MS`）**加**三条 `[UNSOURCED]` 扩展（`FARM_REPO_PATH` /
//! `FARM_WORKTREE_PATH` / `FARM_WORKTREE_BRANCH`——`WorktreeEstablishRequest` 的三条路径
//! 在票面与契约里都没有来源，见 `main_loop` 模块头的 deviation 记录）。缺任何一条都
//! **立刻退出并指名变量**：没有默认值兜底，也没有静默的半装配状态。
//!
//! 信号：`SIGINT` / `SIGTERM` 都干净停下循环，**不**在退出路上交还 claim（claim 是票的
//! 状态不是锁，见 `main_loop` 模块头）。
//!
//! 日志走 `tracing`，级别由 `RUST_LOG` 控制（缺省 `info`）；`alert=` 字段是 ADR-0004 的
//! 标注 / 各类降级的机械可观察点。

use std::process::ExitCode;

use orchestrator::main_loop::{self, OrchestratorConfig};
use tokio::sync::watch;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = match OrchestratorConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            // 配置错误是**启动期**的致命错：打印出来、指明变量、退出非零。
            eprintln!("orchestrator: {error}");
            return ExitCode::from(2);
        }
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signalled = tokio::spawn(async move {
        wait_for_signal().await;
        let _ = shutdown_tx.send(true);
    });

    let result = main_loop::run(config, shutdown_rx).await;
    signalled.abort();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrator: {error}");
            ExitCode::FAILURE
        }
    }
}

/// 等到 `SIGINT` 或 `SIGTERM`。
///
/// `[UNSOURCED]`：票面只写了「信号处理」，没列信号清单。取这两条是「Ctrl-C」与「服务管理器
/// 停服」的常规集；别的信号不做特殊处理（默认处置）。
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut interrupt = match signal(SignalKind::interrupt()) {
            Ok(stream) => stream,
            Err(error) => {
                tracing::warn!(alert = "signal_unavailable", error = error.to_string().as_str(), "SIGINT 无法订阅");
                return;
            }
        };
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(error) => {
                tracing::warn!(alert = "signal_unavailable", error = error.to_string().as_str(), "SIGTERM 无法订阅");
                return;
            }
        };
        tokio::select! {
            _ = interrupt.recv() => tracing::info!(signal = "SIGINT", "停机信号"),
            _ = terminate.recv() => tracing::info!(signal = "SIGTERM", "停机信号"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!(signal = "ctrl_c", "停机信号");
    }
}

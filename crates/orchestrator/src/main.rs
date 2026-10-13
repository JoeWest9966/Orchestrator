//! Orchestrator 二进制入口。
//!
//! 本文件必须存在：canonical manifest（`crates/orchestrator/Cargo.toml` 的 `[[bin]]`）在
//! workspace 落地时即声明了该目标，缺文件 `cargo build` 直接红。
//! 入口的装配（env 五变量 → tick 循环 → WS 入账任务 → 信号）是 ticket #5 的落点，
//! 不在此处提前发明（60-environment-and-risks.md 的 `[UNSOURCED]` 约定）。

fn main() {
    todo!("ticket #5：装配 tick 循环 + WS 入账任务")
}

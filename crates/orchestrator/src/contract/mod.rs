//! 契约客户端（[`ContractClient`]）与契约 HTTP 机器。
//!
//! 演化自 Farm 骨架 `examples/orchestrator-skeleton/src/client.rs`：以契约客户端身份
//! （只过 farm-sdk 类型 + 契约 HTTP API，无私有后门）驱动宿主。每一条 typed 方法对应
//! 一条契约路由，响应一律解析成 farm-sdk 类型，错误一律落闭集 [`ContractError`]。

pub mod client;
pub mod http;
pub mod ws;

#[cfg(test)]
mod tests;

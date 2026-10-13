//! Orchestrator v1 —— Farm 契约的确定性客户端（tick = reconcile → decide → execute）。

pub mod contract;
pub mod exec;
pub mod main_loop;
pub mod recon;
pub mod tick;

#[cfg(test)]
mod test_support;

//! 契约 HTTP/1.1 传输机器（演化自 Farm 骨架 `examples/orchestrator-skeleton/src/client.rs`）。
//!
//! 立场与骨架一致：用最少机器证明契约可达，不引 reqwest。客户端与宿主之间只走
//! 契约 HTTP API，错误一律落闭集 [`ContractError`]。

use std::net::SocketAddr;

use farm_sdk::ContractError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// 极简 HTTP/1.1 请求：返回 `(状态码, body)`。依赖 `Connection: close` 读到 EOF。
///
/// `auth` 是可选的一对 `(header 名, 值)`（契约面 `X-Farm-Token` / 管理面
/// `X-Farm-Admin-Token`）——两个面凭据不同，故 header 名也作为参数，不写死。
pub async fn http_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    auth: Option<(&str, &str)>,
    body: Option<&str>,
) -> std::io::Result<(u16, String)> {
    let mut stream = TcpStream::connect(addr).await?;
    let body = body.unwrap_or("");
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some((header, value)) = auth {
        req.push_str(&format!("{header}: {value}\r\n"));
    }
    if !body.is_empty() {
        req.push_str("Content-Type: application/json\r\n");
    }
    req.push_str(&format!("Content-Length: {}\r\n\r\n{body}", body.len()));
    stream.write_all(req.as_bytes()).await?;
    stream.flush().await?;

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    let payload = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    Ok((status, payload))
}

/// IO 错误 → 闭集 `service_unavailable`（宿主不可达；可重试）。
///
/// 传输失败在契约里**没有**错误码，故一律归到唯一 `is_retryable()` 为真的那个码。
pub fn io_err(e: std::io::Error) -> ContractError {
    ContractError::service_unavailable(format!("host unreachable: {e}"))
}

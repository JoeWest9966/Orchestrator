//! WS 事件流的**长连**机器（`GET /v1/events/stream`）。
//!
//! 演化自 Farm 骨架 `examples/orchestrator-skeleton/src/client.rs` 的手写 WS upgrade +
//! 首帧解析（骨架只读一帧 `hello` 就收工）。本模块把同一台机器长成一条**订阅**：
//! upgrade → 首帧 `hello` → 之后是事件信封的文本帧流（宿主 `farm-host/src/events_ws.rs`：
//! 先 `hello`，再快照，再增量）。
//!
//! 立场仍然是「最少机器」：不引 WS 库，只承接契约面实际会出现的帧形状——
//!
//! * **服务端 → 客户端的数据帧不带掩码**（RFC 6455 §5.1：服务端 MUST NOT 掩码）。带掩码的
//!   帧是不合规的，本机器**报错**而不是猜它的长度与载荷；
//! * 只承接**单帧文本**（`FIN=1` + opcode `0x1`）。契约面的事件信封是短 JSON，宿主的
//!   WS 写入是单帧；分片（continuation `0x0`）与二进制帧一律报错，不静默吞掉；
//! * 控制帧里只认 `close`（`0x8` → 流结束）；`ping` / `pong` 跳过——宿主侧由 axum 自动应答。
//!
//! 本模块只做「把帧变成 `EventEnvelope`」；**入账与决策不在这里**（ADR-0001：WS 只入账、
//! 从不决策）。

use std::net::SocketAddr;

use farm_sdk::events::{EventEnvelope, WsHello};
use farm_sdk::handshake::AuthScheme;
use farm_sdk::{contract_path, ServiceName};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// 事件流路由（路径由契约路由表构造，不手写）。
fn stream_path() -> String {
    contract_path(ServiceName::Events, "/stream")
}

/// 一条已建立的 WS 订阅。
pub struct EventStream {
    stream: TcpStream,
}

impl EventStream {
    /// 建立订阅：手写 upgrade（op token 与 HTTP 面同一个 header，取自 `AuthScheme`）。
    ///
    /// 非 101 一律报错：token 缺/错会先被中间件拒成 401，不会走到这里静默降级。
    pub async fn connect(addr: SocketAddr, token: &str) -> std::io::Result<Self> {
        let header = AuthScheme::default().http_header;
        let mut stream = TcpStream::connect(addr).await?;
        let upgrade = format!(
            "GET {} HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\
             {header}: {token}\r\n\r\n",
            stream_path()
        );
        stream.write_all(upgrade.as_bytes()).await?;
        stream.flush().await?;

        let response = read_http_header(&mut stream).await?;
        let status_line = response.lines().next().unwrap_or_default();
        if !status_line.contains("101") {
            return Err(std::io::Error::other(format!(
                "the host refused the ws upgrade: {status_line}"
            )));
        }
        Ok(Self { stream })
    }

    /// 读首帧 `hello`（宿主订阅建立后立即回的自描述：契约版本 + 可订阅的事件闭集）。
    pub async fn hello(&mut self) -> std::io::Result<WsHello> {
        let text = self
            .next_text()
            .await?
            .ok_or_else(|| std::io::Error::other("the stream closed before the hello frame"))?;
        serde_json::from_str(&text).map_err(|e| {
            std::io::Error::other(format!("the hello frame is not a WsHello: {e}: {text}"))
        })
    }

    /// 读下一条事件信封；`Ok(None)` = 订阅被对端正常关闭（close 帧或 EOF）。
    pub async fn next_envelope(&mut self) -> std::io::Result<Option<EventEnvelope>> {
        let Some(text) = self.next_text().await? else {
            return Ok(None);
        };
        match serde_json::from_str::<EventEnvelope>(&text) {
            Ok(envelope) => Ok(Some(envelope)),
            // 宿主在存储面读不了时回的是闭集错误信封（HTTP 与 WS 同一形状）：
            // 那不是事件，认出来报错，别当成「一串不认识的东西」。
            Err(parse_error) => match serde_json::from_str::<farm_sdk::ContractError>(&text) {
                Ok(error) => Err(std::io::Error::other(format!(
                    "the host reported {} on the stream: {}",
                    error.code.as_str(),
                    error.message
                ))),
                Err(_) => Err(std::io::Error::other(format!(
                    "a stream frame carries neither an EventEnvelope nor a contract error: \
                     {parse_error}: {text}"
                ))),
            },
        }
    }

    /// 读下一个文本帧载荷（跳过控制帧；close/EOF → `None`）。
    async fn next_text(&mut self) -> std::io::Result<Option<String>> {
        loop {
            let Some((fin, opcode, payload)) = read_frame(&mut self.stream).await? else {
                return Ok(None); // 对端关闭（EOF）
            };
            match opcode {
                0x0 | 0x2 => {
                    return Err(std::io::Error::other(format!(
                        "unsupported ws data frame (opcode {opcode:#x}, fin {fin}) on the event stream"
                    )))
                }
                0x1 => {
                    return String::from_utf8(payload)
                        .map(Some)
                        .map_err(|e| std::io::Error::other(format!("ws text frame is not utf-8: {e}")))
                }
                0x8 => return Ok(None), // close
                0x9 | 0xA => continue,  // ping / pong：由 axum 侧处理，这里跳过
                other => {
                    return Err(std::io::Error::other(format!(
                        "unknown ws opcode {other:#x} on the event stream"
                    )))
                }
            }
        }
    }
}

/// 读 HTTP 响应头（到空行为止）。upgrade 之后的字节都还在同一个 TCP 流上。
async fn read_http_header(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut header = Vec::new();
    let mut byte = [0u8; 1];
    while !header.ends_with(b"\r\n\r\n") {
        let read = stream.read(&mut byte).await?;
        if read == 0 {
            return Err(std::io::Error::other(format!(
                "the connection closed during the ws upgrade: {}",
                String::from_utf8_lossy(&header)
            )));
        }
        header.extend_from_slice(&byte[..read]);
    }
    Ok(String::from_utf8_lossy(&header).into_owned())
}

/// 读一个 WS 帧：`Ok(None)` = 读到 EOF（对端关闭且没有 close 帧）。
///
/// 返回 `(FIN 位, opcode, 载荷)`。掩码位**必须**是 0（服务端帧不带掩码，RFC 6455 §5.1）：
/// 带掩码就不猜，直接报错。
async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Option<(bool, u8, Vec<u8>)>> {
    let mut head = [0u8; 2];
    match stream.read_exact(&mut head).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let fin = head[0] & 0x80 != 0;
    let opcode = head[0] & 0x0F;
    if head[1] & 0x80 != 0 {
        return Err(std::io::Error::other(
            "the host sent a masked ws frame; server-to-client frames must not be masked \
             (RFC 6455 §5.1)",
        ));
    }
    let length = match head[1] & 0x7F {
        n @ 0..=125 => u64::from(n),
        126 => {
            let mut ext = [0u8; 2];
            stream.read_exact(&mut ext).await?;
            u64::from(u16::from_be_bytes(ext))
        }
        127 => {
            let mut ext = [0u8; 8];
            stream.read_exact(&mut ext).await?;
            u64::from_be_bytes(ext)
        }
        _ => unreachable!("7-bit length is at most 127"),
    };
    let mut payload = vec![0u8; usize::try_from(length).unwrap_or(usize::MAX)];
    stream.read_exact(&mut payload).await?;
    Ok(Some((fin, opcode, payload)))
}

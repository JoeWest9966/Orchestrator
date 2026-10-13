//! 测试进程级的一次性装配。
//!
//! ## 为什么需要它
//!
//! `tracing` 的 **callsite interest 缓存是进程级的**，不是 per-subscriber 的：某个
//! `tracing::warn!` 调用点第一次被求值时，`tracing` 问「当时的 dispatcher 对这个调用点
//! 感兴趣吗」，并把答案**缓存住**。如果那一刻既没有线程局部默认 subscriber
//! （`set_default` 装的），也没有全局默认 subscriber，答案就是「不感兴趣」——于是这个
//! 调用点之后的事件**全部被丢弃**，哪怕后来 `set_default` 装了收集器。
//!
//! 并发跑 `cargo test` 时，另一个测试线程可能先撞上同一个调用点（那时它自己的线程上
//! 没有 subscriber），把「不感兴趣」缓存下来；本测试随后就收不到告警。实测：单独跑
//! `exec::tests::every_closed_set_error_code_takes_its_own_branch_in_the_sequence`
//! 60/60 通过，整套 lib 测试并发跑约 3% 的概率挂掉（`告警一次` 断言 left: 0 / right: 1）。
//!
//! ## 修法
//!
//! 装一个**全局 no-op subscriber**，让进程里永远有一个 dispatcher，interest 缓存里就
//! 不会出现「不感兴趣」。线程局部的 `set_default` 收集器仍然优先于它，所以告警照样
//! 收得到——只是不再有「冷缓存把事件吞掉」这条路径。
//!
//! `enabled` 必须返回 `true`：返回 `false` 会让缓存写成「不感兴趣」，正是要避的那个坑。

use std::sync::Once;

static INSTALL: Once = Once::new();

/// 幂等地装上全局 no-op subscriber。在每条收集告警的测试开头调用。
pub fn install_global_noop_subscriber() {
    INSTALL.call_once(|| {
        // 已有全局默认时 `set_global_default` 返回 Err——那正是我们要的状态，忽略即可。
        let _ = tracing::subscriber::set_global_default(NoopSubscriber);
    });
}

/// 什么都不做、但对一切调用点「感兴趣」的 subscriber。
#[derive(Clone, Copy, Default)]
struct NoopSubscriber;

impl tracing::Subscriber for NoopSubscriber {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, _: &tracing::Event<'_>) {}

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installing_the_noop_subscriber_is_idempotent_and_keeps_callsites_interested() {
        install_global_noop_subscriber();
        install_global_noop_subscriber();

        // 进程里有了 dispatcher：`tracing::dispatcher::has_been_set` 为真意味着
        // interest 缓存不会再把调用点冷缓存成「不感兴趣」。
        assert!(
            tracing::dispatcher::has_been_set(),
            "a global default subscriber must be installed so no callsite is cold-cached as uninterested"
        );
    }
}

// ---------------------------------------------------------------------------
// 假宿主共用的 HTTP 请求读取器
// ---------------------------------------------------------------------------

/// 假宿主收到的一条请求（三个单测模块原来各抄了一份，现在只有这一份）。
///
/// `path` 是**完整请求目标**（含查询串）；要断言路由用 [`CapturedRequest::route`]。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CapturedRequest {
    pub method: String,
    pub path: String,
    /// 小写化的 header 名 → 值（鉴权断言用）。
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl CapturedRequest {
    /// 查询串剥掉后的路径（路由断言用）。
    pub fn route(&self) -> &str {
        self.path.split('?').next().unwrap_or(&self.path)
    }

    /// 取一个 header（名字大小写不敏感；存的时候已小写化）。
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// 从 socket 读一条完整的 HTTP 请求（头 + `Content-Length` 指定的体）。
///
/// 对端在读完之前就断开 ⇒ 返回 [`CapturedRequest::default`]（与原来的三份实现同款：不猜）。
pub async fn read_request(socket: &mut tokio::net::TcpStream) -> CapturedRequest {
    use tokio::io::AsyncReadExt;

    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    let head_end = loop {
        let read = socket.read(&mut chunk).await.unwrap_or(0);
        if read == 0 {
            return CapturedRequest::default();
        }
        buf.extend_from_slice(&chunk[..read]);
        if let Some(end) = find_subslice(&buf, b"\r\n\r\n") {
            break end;
        }
    };

    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or_default();
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

    CapturedRequest {
        method,
        path,
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    }
}

/// 在 `haystack` 里找 `needle` 第一次出现的下标。
pub fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

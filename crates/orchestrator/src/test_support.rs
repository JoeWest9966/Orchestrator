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

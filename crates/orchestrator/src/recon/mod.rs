//! recon —— 世界模型与对账重建（ADR-0002：零持久状态）。
//!
//! 编排器不落任何本地状态：进程内只有 [`WorldModel`]，进程死掉那一刻的世界被视为可再生数据。
//! 每个 tick 宿主契约面被读三份只读快照（ticket #2 范围）：
//!
//! 1. `GET /v1/tracker/issues` 快照 —— `assignee == FARM_ASSIGNEE` 即我的 claim（claim 判定）；
//! 2. `GET /v1/events/history`（增量，`since = event_watermark`）—— 活跃 attempt / 完成信号 / 冷却起点；
//! 3. `GET /v1/workspace/mappings` —— 已建 worktree 与集成分支。
//!
//! **世界只由前两份推出**：第三份与 attempt 的生命周期正交（见下文）。
//!
//! **时间戳形状**：本模块产出的时间戳一律是宿主事件时间戳的形状（RFC3339、UTC、秒级，如
//! `2026-01-01T00:00:00Z`；宿主 `now_rfc3339` 写入，`event_log.rs::canonical_timestamp` 对查询
//! 参数做同一归一）。这个形状是「字典序即时间序」的前提，所以 `event_watermark` 可以直接喂给
//! 下一次 `EventHistoryQuery.since`，`backoff_until` 可以直接与 world 里的 now 比较。
//!
//! `WorldModel` 的五个字段全部来自前两个源。第三份快照（workspace mappings）与 attempt 的
//! 生命周期正交——worktree 在 claim 之后、dispatch 之前就已存在，`WorktreeState` 也不描述尝试——
//! 所以它**不是任何字段的输入**，[`rebuild`] 也不收它；worktree 的复用判定归 decide / exec
//! （ticket #3/#4）。
//!
//! ## 本模块不告警
//!
//! 环依赖（[`blocked_by_cycles`]）与保留窗口截断的**告警不在这里发**（ticket #6）：两者说的都是
//! **世界的事实**（环会一直成环、宿主忙时最近一页会一直被截断），不随 tick 翻转，所以「同一
//! 事实只告警一次」的去重位只能在调用方；而纯函数记不住自己说过什么，调用方也没法给自己没印发
//! 过的告警去重——于是判据留给本模块（数据），发放与去重归 `main_loop`
//! （[`AlertDedup`](crate::main_loop::AlertDedup)）。本模块因此保持「数据进、数据出」：零 IO、
//! 零持久状态、零日志副作用。

use std::collections::{BTreeMap, BTreeSet};

use farm_sdk::events::EventKind;
use farm_sdk::services::events::EventHistoryResponse;
use farm_sdk::services::tracker::{IssueListResponse, IssueState};
use farm_sdk::workflow::RetryBackoff;
use serde::{Deserialize, Serialize};

/// 对账重建出的世界模型：ticket #2 穷举的**五字段闭集**。
///
/// 它是**派生状态**，不是 wire 类型——每个字段都能从上面三份契约快照推出，字段类型全是
/// 契约里的原语值（票 id / attempt id / RFC3339 时间戳），没有一个字段是契约 schema 之外发明出来的。
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorldModel {
    /// `IssueView.assignee == FARM_ASSIGNEE` 的票（我的 claim）。
    pub claimed_by_me: BTreeSet<String>,
    /// 每票最后一条 `dispatch` 之后**终态事件缺席**的活跃 attempt：
    /// 票 id → `EventEnvelope.attempt_id`（dispatch 未带 attempt id 时是 `None`）。
    pub active_attempts: BTreeMap<String, Option<String>>,
    /// 完成信号：看到 `merge` 事件（窗口截断时回退 issues 快照的 `state == done`）。
    pub merge_done: BTreeSet<String>,
    /// 增量事件拉取的游标：最后一条已消费事件的 `occurred_at`（RFC3339，`since` 的时间戳游标）。
    pub event_watermark: Option<String>,
    /// 冷却截止时间：失败 / 判杀 / `interrupted` 之后，按 effective `RetryBackoff` 算出的时间戳。
    pub backoff_until: BTreeMap<String, String>,
}

/// 对账重建（纯函数：零 IO、零持久状态、零日志）。
///
/// `assignee` 是 `FARM_ASSIGNEE`；`previous_watermark` 是上一次 world 的 `event_watermark`
/// （本次增量拉取用的 `since`），空页时游标不回退；`retry_backoff` 是 effective 配置里的
/// `WorkflowConfig::retry_backoff`（冷却只消费它，不自带常数）。
///
/// 输入里**没有** workspace mappings：它与 attempt 的生命周期正交（见模块头），世界用不到它。
/// 环依赖（[`blocked_by_cycles`]）与保留窗口截断的告警也不在这里发——见模块头「本模块不告警」
/// （ticket #6）。
pub fn rebuild(
    assignee: &str,
    issues: &IssueListResponse,
    history: &EventHistoryResponse,
    previous_watermark: Option<&str>,
    retry_backoff: &RetryBackoff,
) -> WorldModel {
    let claimed_by_me = issues
        .issues
        .iter()
        .filter(|issue| issue.assignee.as_deref() == Some(assignee))
        .map(|issue| issue.issue_id.clone())
        .collect();

    let mut active_attempts: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut merge_done: BTreeSet<String> = BTreeSet::new();
    let mut backoff_until: BTreeMap<String, String> = BTreeMap::new();
    let mut cooldown_triggers: BTreeMap<&str, u32> = BTreeMap::new();

    // 宿主的事件历史按 `seq` 升序返回（`event_log.rs::history`），直接顺序折叠即可。
    for event in &history.events {
        let Some(ticket_id) = event.ticket_id.as_deref() else {
            // 无票事件（`host_restarted`、插件事件等）：只登记，不碰 per-票状态。
            continue;
        };
        if ends_the_attempt(event.kind) {
            active_attempts.remove(ticket_id);
        }
        if event.kind == EventKind::Dispatch {
            active_attempts.insert(ticket_id.to_string(), event.attempt_id.clone());
        }
        if event.kind == EventKind::Merge {
            merge_done.insert(ticket_id.to_string());
        }
        if starts_a_cooldown(event.kind) {
            let trigger = cooldown_triggers.entry(ticket_id).or_insert(0);
            *trigger += 1;
            let delay_ms = cooldown_delay_ms(retry_backoff, *trigger);
            backoff_until.insert(
                ticket_id.to_string(),
                cooldown_deadline(&event.occurred_at, delay_ms),
            );
        }
    }

    // 保留窗口截断：事件账不全 ⇒ 完成信号回退 issues 快照的 `state == done`（ticket #2 的
    // 退化路径）。绝不从缺失的事件里凭空补状态——推不出的就留给下一个 tick。
    //
    // 截断的**告警**由调用方发（ticket #6）：本函数每次调用都从零推世界，没法记住「这条说过了」。
    if history.truncated {
        for issue in &issues.issues {
            if issue.state == IssueState::Done {
                merge_done.insert(issue.issue_id.clone());
            }
        }
    }

    WorldModel {
        claimed_by_me,
        active_attempts,
        merge_done,
        event_watermark: history
            .events
            .last()
            .map(|event| event.occurred_at.clone())
            .or_else(|| previous_watermark.map(str::to_string)),
        backoff_until,
    }
}

/// `blocked_by` 里成环的票组（每组一个强连通分量，含自环）——按票 id 排序，确定性地报告。
///
/// 边 = `IssueView.blocked_by`（加法字段，声明方向：堵住本票的票）。只在快照里的票才有出边；
/// 被引用而不在快照里的票（未取到 / 不在视图）当作无出边的节点，不猜它们的依赖。
///
/// **判据在这里，告警不在这里**（ticket #6）：本函数是纯函数、零持久状态（ADR-0002），每次
/// 调用都会重新推出同一个环，所以「环只告警一次」的去重与发放都只能由调用方做
/// （`main_loop` 的 [`AlertDedup`](crate::main_loop::AlertDedup)）。本函数只回答「这份快照里
/// 有哪些环」这一个问题。
pub fn blocked_by_cycles(issues: &IssueListResponse) -> Vec<Vec<String>> {
    let mut ids: BTreeSet<&str> = BTreeSet::new();
    for issue in &issues.issues {
        ids.insert(issue.issue_id.as_str());
        for blocker in &issue.blocked_by {
            ids.insert(blocker.as_str());
        }
    }
    let ids: Vec<&str> = ids.into_iter().collect();
    let index: BTreeMap<&str, usize> = ids.iter().enumerate().map(|(i, id)| (*id, i)).collect();
    let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); ids.len()];
    for issue in &issues.issues {
        let from = index[issue.issue_id.as_str()];
        for blocker in &issue.blocked_by {
            adjacency[from].push(index[blocker.as_str()]);
        }
    }

    let mut groups: Vec<Vec<String>> = Vec::new();
    for component in strongly_connected_components(&adjacency) {
        let self_loop = component.iter().any(|node| adjacency[*node].contains(node));
        if component.len() > 1 || self_loop {
            let mut group: Vec<String> = component
                .iter()
                .map(|node| ids[*node].to_string())
                .collect();
            group.sort();
            groups.push(group);
        }
    }
    groups.sort();
    groups
}

/// Tarjan 强连通分量（迭代式：票图可以任意长，递归会爆栈）。
///
/// 返回每个分量里的节点下标；分量内部顺序不定，由调用方排序。
fn strongly_connected_components(adjacency: &[Vec<usize>]) -> Vec<Vec<usize>> {
    enum Step {
        Visit(usize),
        Finish,
    }

    let mut index_of: Vec<Option<usize>> = vec![None; adjacency.len()];
    let mut lowlink: Vec<usize> = vec![0; adjacency.len()];
    let mut on_stack: Vec<bool> = vec![false; adjacency.len()];
    let mut stack: Vec<usize> = Vec::new();
    let mut call_stack: Vec<(usize, usize)> = Vec::new();
    let mut components: Vec<Vec<usize>> = Vec::new();
    let mut next_index = 0usize;

    for root in 0..adjacency.len() {
        if index_of[root].is_some() {
            continue;
        }
        index_of[root] = Some(next_index);
        lowlink[root] = next_index;
        next_index += 1;
        stack.push(root);
        on_stack[root] = true;
        call_stack.push((root, 0));

        while let Some(&(node, _)) = call_stack.last() {
            let step = {
                let frame = call_stack.last_mut().expect("刚读过，非空");
                match adjacency[node].get(frame.1).copied() {
                    Some(neighbour) => {
                        frame.1 += 1;
                        Step::Visit(neighbour)
                    }
                    None => Step::Finish,
                }
            };
            match step {
                Step::Visit(neighbour) => {
                    if index_of[neighbour].is_none() {
                        index_of[neighbour] = Some(next_index);
                        lowlink[neighbour] = next_index;
                        next_index += 1;
                        stack.push(neighbour);
                        on_stack[neighbour] = true;
                        call_stack.push((neighbour, 0));
                    } else if on_stack[neighbour] {
                        let neighbour_index = index_of[neighbour].expect("已访问过");
                        lowlink[node] = lowlink[node].min(neighbour_index);
                    }
                }
                Step::Finish => {
                    call_stack.pop();
                    if let Some(&(parent, _)) = call_stack.last() {
                        lowlink[parent] = lowlink[parent].min(lowlink[node]);
                    }
                    if lowlink[node] == index_of[node].expect("已访问过") {
                        let mut component = Vec::new();
                        while let Some(member) = stack.pop() {
                            on_stack[member] = false;
                            component.push(member);
                            if member == node {
                                break;
                            }
                        }
                        components.push(component);
                    }
                }
            }
        }
    }
    components
}

/// 终态事件：结束该票当前活跃的 attempt。
///
/// 闭集来自宿主 agent-runner 的收尾映射（`farm-core/src/agent_runner/service.rs:755-760`：
/// `settled → agent_settled`、`aborted → abort`、`interrupted → interrupted`、
/// `failed → attempt_finished`）加上 `stall_kill`（判杀）与 `merge`（完成）。
/// `blocked` / `needs_attention` **不是**终态：前者语义就是「attempt 卡住、claim 保留」。
fn ends_the_attempt(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::AgentSettled
            | EventKind::AttemptFinished
            | EventKind::Abort
            | EventKind::Interrupted
            | EventKind::StallKill
            | EventKind::Merge
    )
}

/// 冷却触发：失败（`attempt_finished`，宿主把 `AttemptState::Failed` 收尾成它）/ 判杀 / `interrupted`。
fn starts_a_cooldown(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::AttemptFinished | EventKind::StallKill | EventKind::Interrupted
    )
}

/// 第 `trigger` 次冷却触发（1 起算）的延迟毫秒：`base_delay_ms * backoff_multiplier^(trigger-1)`。
///
/// 指数以 `max_retries - 1` 封顶（档位用完后不循环、不加码——宿主插件退避的同一精神）；
/// 三个常数全部来自 effective `RetryBackoff`，本模块不自带退避常数。
fn cooldown_delay_ms(retry_backoff: &RetryBackoff, trigger: u32) -> u64 {
    let multiplier = u64::from(retry_backoff.backoff_multiplier.max(1));
    let last_tier = retry_backoff.max_retries.saturating_sub(1);
    let exponent = trigger.saturating_sub(1).min(last_tier);
    retry_backoff
        .base_delay_ms
        .saturating_mul(multiplier.saturating_pow(exponent))
}

/// 冷却截止时间戳 = 触发时刻 + 档位延迟（秒级，与宿主事件时间戳同形状）。
///
/// 亚秒延迟按宿主 `canonical_timestamp` 的同一做法截断到秒；触发时刻不是宿主形状
/// （带偏移量、亚秒精度等）时不猜延迟，记触发时刻本身——退避丢失可接受，编造时间戳不可接受。
///
/// `pub(crate)`：tick 循环（`crate::main_loop`）把 `exec` 失败回滚交回的 [`Cooldown`]
/// 折进世界时，也走这一处换算——冷却的截止时刻算术只有这一处实现。
///
/// [`Cooldown`]: crate::exec::Cooldown
pub(crate) fn cooldown_deadline(occurred_at: &str, delay_ms: u64) -> String {
    match parse_host_timestamp(occurred_at) {
        Some(epoch_secs) => format_host_timestamp(epoch_secs + (delay_ms / 1_000) as i64),
        None => occurred_at.to_string(),
    }
}

/// 解析宿主事件时间戳形状（`YYYY-MM-DDTHH:MM:SSZ`，秒级 UTC）为 Unix 秒。
fn parse_host_timestamp(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    if bytes.len() != 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
    {
        return None;
    }
    let year: i32 = text[0..4].parse().ok()?;
    let month: u32 = text[5..7].parse().ok()?;
    let day: u32 = text[8..10].parse().ok()?;
    let hour: i64 = text[11..13].parse().ok()?;
    let minute: i64 = text[14..16].parse().ok()?;
    let second: i64 = text[17..19].parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second)
}

/// Unix 秒 → 宿主事件时间戳形状。
///
/// `pub(crate)`：tick 循环（`crate::main_loop`）注入的钟面必须与本模块产出的时间戳同形，
/// 形状的日历换算只有这一处实现（「字典序即时间序」的前提只有一处需要成立）。
pub(crate) fn format_host_timestamp(epoch_secs: i64) -> String {
    let days = epoch_secs.div_euclid(86_400);
    let tod = epoch_secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (tod / 3_600, (tod % 3_600) / 60, tod % 60);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// 儒略日式的日历换算（Howard Hinnant 的 `days_from_civil`）：公历年月日 → 距 1970-01-01 的天数。
fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = i64::from(if month <= 2 { year - 1 } else { year });
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = i64::from((month + 9) % 12);
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// 上式的逆：距 1970-01-01 的天数 → 公历年月日。
fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    (
        (year + i64::from(month <= 2)) as i32,
        month as u32,
        day as u32,
    )
}

#[cfg(test)]
mod tests {
    use farm_sdk::events::EventEnvelope;
    use farm_sdk::services::tracker::IssueView;

    use super::*;

    fn issue(issue_id: &str, state: IssueState, assignee: Option<&str>) -> IssueView {
        IssueView {
            issue_id: issue_id.to_string(),
            title: issue_id.to_string(),
            state,
            labels: vec!["dispatch".to_string()],
            assignee: assignee.map(str::to_string),
            updated_at: None,
            blocked_by: Vec::new(),
            blocking: Vec::new(),
        }
    }

    fn event(id: &str, kind: EventKind, occurred_at: &str) -> EventEnvelope {
        EventEnvelope {
            id: id.to_string(),
            kind,
            occurred_at: occurred_at.to_string(),
            actor: "host".to_string(),
            ticket_id: None,
            attempt_id: None,
            payload: serde_json::Value::Null,
            audited: false,
        }
    }

    fn for_ticket(
        mut envelope: EventEnvelope,
        ticket_id: &str,
        attempt_id: Option<&str>,
    ) -> EventEnvelope {
        envelope.ticket_id = Some(ticket_id.to_string());
        envelope.attempt_id = attempt_id.map(str::to_string);
        envelope
    }

    #[test]
    fn a_host_restart_marks_the_running_attempt_interrupted_and_starts_a_cooldown() {
        // 崩溃重启场景：running attempt 被宿主标成 `interrupted`（第四终态），同一次重启还发 `host_restarted`。
        let issues = IssueListResponse {
            issues: vec![issue("T1", IssueState::Claimed, Some("orch-1"))],
        };
        let history = EventHistoryResponse {
            events: vec![
                for_ticket(
                    event("e1", EventKind::Dispatch, "2026-01-01T00:00:07Z"),
                    "T1",
                    Some("a1"),
                ),
                event("e2", EventKind::HostRestarted, "2026-01-01T00:00:08Z"),
                for_ticket(
                    event("e3", EventKind::Interrupted, "2026-01-01T00:00:09Z"),
                    "T1",
                    Some("a1"),
                ),
            ],
            truncated: false,
        };
        // 冷却档位：首档 = base_delay_ms（1s），退避倍率 2。
        let backoff = RetryBackoff {
            max_retries: 3,
            base_delay_ms: 1_000,
            backoff_multiplier: 2,
        };

        let world = rebuild("orch-1", &issues, &history, None, &backoff);

        assert_eq!(world.claimed_by_me, BTreeSet::from(["T1".to_string()]));
        assert!(
            world.active_attempts.is_empty(),
            "interrupted 是终态：崩溃后不该再有活跃 attempt"
        );
        assert!(world.merge_done.is_empty(), "没有 merge 事件就没有完成信号");
        assert_eq!(
            world.backoff_until.get("T1").map(String::as_str),
            Some("2026-01-01T00:00:10Z"),
            "冷却 = interrupted 时刻 + 首档 1s"
        );
        assert_eq!(
            world.event_watermark.as_deref(),
            Some("2026-01-01T00:00:09Z")
        );
    }

    #[test]
    fn a_truncated_history_falls_back_to_the_issues_snapshot() {
        // 窗口截断场景：`truncated == true` 是保留窗口唯一可见的信号（ADR-0005 §10）。
        // 事件账不全 ⇒ 完成信号只能从 issues 快照兜底；不可用的事件簿不猜任何东西。
        let issues = IssueListResponse {
            issues: vec![
                issue("T1", IssueState::Done, Some("orch-1")),
                issue("T2", IssueState::Claimed, Some("orch-1")),
            ],
        };
        let history = EventHistoryResponse {
            events: Vec::new(),
            truncated: true,
        };
        let backoff = RetryBackoff {
            max_retries: 3,
            base_delay_ms: 1_000,
            backoff_multiplier: 2,
        };

        let world = rebuild(
            "orch-1",
            &issues,
            &history,
            Some("2026-01-01T00:00:05Z"),
            &backoff,
        );

        assert_eq!(
            world.merge_done,
            BTreeSet::from(["T1".to_string()]),
            "窗口截断时，票的 `done` 状态就是完成信号"
        );
        assert_eq!(
            world.claimed_by_me,
            BTreeSet::from(["T1".to_string(), "T2".to_string()])
        );
        assert!(
            world.active_attempts.is_empty(),
            "窗口里没有 dispatch，就不凭空推出活跃 attempt"
        );
        assert!(world.backoff_until.is_empty(), "没有失败事件就没有冷却");
        assert_eq!(
            world.event_watermark.as_deref(),
            Some("2026-01-01T00:00:05Z"),
            "空页不回退游标"
        );
    }

    #[test]
    fn a_blocked_by_cycle_does_not_panic_and_leaves_the_rest_of_the_world_alone() {
        // 环依赖票：重建不得 panic、不得挂死，环上的票永远算不出 frontier（ticket #2 验收第 3 条）。
        // 告警的发放与去重不在这里——见模块头「本模块不告警」（ticket #6）。
        let mut blocked_a = issue("A", IssueState::Open, None);
        blocked_a.blocked_by = vec!["B".to_string()];
        let mut blocked_b = issue("B", IssueState::Open, None);
        blocked_b.blocked_by = vec!["C".to_string()];
        let mut blocked_c = issue("C", IssueState::Open, None);
        blocked_c.blocked_by = vec!["A".to_string()];
        // 环外一张票：它只是依赖环里的票，自己不在环上。
        let mut mine = issue("S", IssueState::Claimed, Some("orch-1"));
        mine.blocked_by = vec!["A".to_string()];

        let issues = IssueListResponse {
            issues: vec![blocked_a, blocked_b, blocked_c, mine],
        };
        let history = EventHistoryResponse {
            events: Vec::new(),
            truncated: false,
        };
        let backoff = RetryBackoff {
            max_retries: 3,
            base_delay_ms: 1_000,
            backoff_multiplier: 2,
        };

        let world = rebuild("orch-1", &issues, &history, None, &backoff);

        assert_eq!(
            blocked_by_cycles(&issues),
            vec![vec!["A".to_string(), "B".to_string(), "C".to_string()]],
            "点名的只有环上的票：依赖环的 S 不在环上"
        );
        assert_eq!(
            world.claimed_by_me,
            BTreeSet::from(["S".to_string()]),
            "环不影响其余重建结果"
        );
    }

    #[test]
    fn a_ticket_claimed_by_someone_else_is_not_claimed_by_me() {
        let issues = IssueListResponse {
            issues: vec![
                issue("T1", IssueState::Claimed, Some("orch-1")),
                issue("T2", IssueState::Claimed, Some("orch-2")),
                issue("T3", IssueState::Open, None),
            ],
        };
        let history = EventHistoryResponse {
            events: Vec::new(),
            truncated: false,
        };
        let backoff = RetryBackoff::default();

        let world = rebuild("orch-1", &issues, &history, None, &backoff);

        assert_eq!(
            world.claimed_by_me,
            BTreeSet::from(["T1".to_string()]),
            "别人的 claim 不是我的，没人 claim 的也不是"
        );
    }

    #[test]
    fn a_merge_event_marks_the_ticket_done_and_removes_the_active_attempt() {
        let issues = IssueListResponse {
            issues: vec![issue("T1", IssueState::Claimed, Some("orch-1"))],
        };
        let history = EventHistoryResponse {
            events: vec![
                for_ticket(
                    event("e1", EventKind::Dispatch, "2026-01-01T00:00:07Z"),
                    "T1",
                    Some("a1"),
                ),
                for_ticket(
                    event("e2", EventKind::Merge, "2026-01-01T00:00:31Z"),
                    "T1",
                    Some("a1"),
                ),
            ],
            truncated: false,
        };
        let backoff = RetryBackoff {
            max_retries: 3,
            base_delay_ms: 1_000,
            backoff_multiplier: 2,
        };

        let world = rebuild("orch-1", &issues, &history, None, &backoff);

        assert_eq!(world.merge_done, BTreeSet::from(["T1".to_string()]));
        assert!(
            world.active_attempts.is_empty(),
            "merge 之后这张票不再占并发槽"
        );
        assert!(
            world.backoff_until.is_empty(),
            "merge 是完成信号，不是失败：不冷却"
        );
    }

    #[test]
    fn the_event_watermark_advances_to_the_last_consumed_event() {
        let issues = IssueListResponse { issues: Vec::new() };
        let history = EventHistoryResponse {
            events: vec![
                for_ticket(
                    event("e1", EventKind::Dispatch, "2026-01-01T00:00:07Z"),
                    "T1",
                    Some("a1"),
                ),
                for_ticket(
                    event("e2", EventKind::AgentSettled, "2026-01-01T00:00:09Z"),
                    "T1",
                    Some("a1"),
                ),
            ],
            truncated: false,
        };
        let backoff = RetryBackoff::default();

        let world = rebuild(
            "orch-1",
            &issues,
            &history,
            Some("2026-01-01T00:00:00Z"),
            &backoff,
        );

        assert_eq!(
            world.event_watermark.as_deref(),
            Some("2026-01-01T00:00:09Z"),
            "游标 = 最后一条已消费事件的 occurred_at（刷新下一次 since 的下界）"
        );
    }

    #[test]
    fn cooldown_tiers_follow_the_effective_retry_backoff() {
        // 冷却只消费 effective `RetryBackoff`：首档 = base_delay_ms，下一档 × backoff_multiplier。
        // T1 触发一次（1 档），T2 触发两次（2 档，跨月末）。
        let issues = IssueListResponse {
            issues: vec![
                issue("T1", IssueState::InProgress, Some("orch-1")),
                issue("T2", IssueState::InProgress, Some("orch-1")),
            ],
        };
        let history = EventHistoryResponse {
            events: vec![
                for_ticket(
                    event("e1", EventKind::Dispatch, "2026-01-01T00:00:00Z"),
                    "T1",
                    Some("a1"),
                ),
                for_ticket(
                    event("e2", EventKind::StallKill, "2026-01-01T00:00:01Z"),
                    "T1",
                    Some("a1"),
                ),
                for_ticket(
                    event("e3", EventKind::Dispatch, "2026-01-01T00:00:10Z"),
                    "T2",
                    Some("b1"),
                ),
                for_ticket(
                    event("e4", EventKind::StallKill, "2026-01-01T00:00:11Z"),
                    "T2",
                    Some("b1"),
                ),
                for_ticket(
                    event("e5", EventKind::Dispatch, "2026-01-01T00:00:20Z"),
                    "T2",
                    Some("b2"),
                ),
                for_ticket(
                    event("e6", EventKind::Interrupted, "2026-01-31T23:59:59Z"),
                    "T2",
                    Some("b2"),
                ),
            ],
            truncated: false,
        };
        let backoff = RetryBackoff {
            max_retries: 3,
            base_delay_ms: 2_000,
            backoff_multiplier: 2,
        };

        let world = rebuild("orch-1", &issues, &history, None, &backoff);

        assert_eq!(
            world.backoff_until,
            BTreeMap::from([
                ("T1".to_string(), "2026-01-01T00:00:03Z".to_string()),
                ("T2".to_string(), "2026-02-01T00:00:03Z".to_string()),
            ]),
            "T1 首档 2s；T2 第二档 4s（2s × 2），跨月末"
        );
        assert!(
            world.active_attempts.is_empty(),
            "stall_kill / interrupted 都是终态"
        );
        assert_eq!(
            world.event_watermark.as_deref(),
            Some("2026-01-31T23:59:59Z")
        );
    }

    #[test]
    fn blocked_and_needs_attention_are_registered_without_ending_the_attempt() {
        // 一等事件 `blocked` / `needs_attention` 只登记（接线是 ticket #4 的活）：
        // `blocked` 的契约语义就是「attempt 卡住、claim 保留」，所以它不结束活跃 attempt。
        let issues = IssueListResponse {
            issues: vec![issue("T1", IssueState::Blocked, Some("orch-1"))],
        };
        let history = EventHistoryResponse {
            events: vec![
                for_ticket(
                    event("e1", EventKind::Dispatch, "2026-01-01T00:00:00Z"),
                    "T1",
                    Some("a1"),
                ),
                for_ticket(
                    event("e2", EventKind::Blocked, "2026-01-01T00:00:01Z"),
                    "T1",
                    Some("a1"),
                ),
                for_ticket(
                    event("e3", EventKind::NeedsAttention, "2026-01-01T00:00:02Z"),
                    "T1",
                    Some("a1"),
                ),
            ],
            truncated: false,
        };
        let backoff = RetryBackoff {
            max_retries: 3,
            base_delay_ms: 2_000,
            backoff_multiplier: 2,
        };

        let world = rebuild("orch-1", &issues, &history, None, &backoff);

        assert_eq!(
            world.active_attempts,
            BTreeMap::from([("T1".to_string(), Some("a1".to_string()))]),
            "blocked 的 attempt 还在跑，仍占并发槽"
        );
        assert!(world.backoff_until.is_empty(), "登记类事件不产生冷却");
        assert!(world.merge_done.is_empty());
    }

    #[test]
    fn the_world_model_carries_exactly_the_five_contract_derived_fields() {
        // 五字段是闭集（ticket #2「范围」穷举）：多一个字段就是违约。
        // 并且每个叶子都是契约里的原语值（票 id / attempt id / RFC3339 时间戳）。
        let issues = IssueListResponse {
            issues: vec![issue("T1", IssueState::Done, Some("orch-1"))],
        };
        let history = EventHistoryResponse {
            events: vec![
                for_ticket(
                    event("e1", EventKind::Dispatch, "2026-01-01T00:00:07Z"),
                    "T1",
                    Some("a1"),
                ),
                for_ticket(
                    event("e2", EventKind::Merge, "2026-01-01T00:00:09Z"),
                    "T1",
                    Some("a1"),
                ),
            ],
            truncated: false,
        };
        let backoff = RetryBackoff {
            max_retries: 3,
            base_delay_ms: 2_000,
            backoff_multiplier: 2,
        };

        let world = rebuild("orch-1", &issues, &history, None, &backoff);
        let json = serde_json::to_value(&world).expect("world 可序列化");

        let keys: BTreeSet<&str> = json
            .as_object()
            .expect("world 是对象")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            BTreeSet::from([
                "claimed_by_me",
                "active_attempts",
                "merge_done",
                "event_watermark",
                "backoff_until",
            ]),
            "五字段闭集"
        );
        assert_all_leaves_are_primitives(&json);
    }

    /// 递归断言 JSON 的每个叶子都是字符串或 null（没有嵌进来的契约 wire 对象）。
    fn assert_all_leaves_are_primitives(value: &serde_json::Value) {
        match value {
            serde_json::Value::Object(entries) => {
                for entry in entries.values() {
                    assert_all_leaves_are_primitives(entry);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    assert_all_leaves_are_primitives(item);
                }
            }
            serde_json::Value::String(_) | serde_json::Value::Null => {}
            other => panic!("world 里的叶子必须是契约原语值，得到 {other:?}"),
        }
    }

    #[test]
    fn cooldown_tiers_do_not_escalate_past_the_retry_budget() {
        // 档位用完后不循环、不加码（宿主插件退避的同一精神）：max_retries = 1 只有首档。
        let issues = IssueListResponse {
            issues: vec![issue("T1", IssueState::InProgress, Some("orch-1"))],
        };
        let history = EventHistoryResponse {
            events: vec![
                for_ticket(
                    event("e1", EventKind::Dispatch, "2026-01-01T00:00:00Z"),
                    "T1",
                    Some("a1"),
                ),
                for_ticket(
                    event("e2", EventKind::StallKill, "2026-01-01T00:00:01Z"),
                    "T1",
                    Some("a1"),
                ),
                for_ticket(
                    event("e3", EventKind::Dispatch, "2026-01-01T00:00:02Z"),
                    "T1",
                    Some("a2"),
                ),
                for_ticket(
                    event("e4", EventKind::StallKill, "2026-01-01T00:00:03Z"),
                    "T1",
                    Some("a2"),
                ),
                for_ticket(
                    event("e5", EventKind::Dispatch, "2026-01-01T00:00:04Z"),
                    "T1",
                    Some("a3"),
                ),
                for_ticket(
                    event("e6", EventKind::Interrupted, "2026-01-01T00:00:05Z"),
                    "T1",
                    Some("a3"),
                ),
            ],
            truncated: false,
        };
        let backoff = RetryBackoff {
            max_retries: 1,
            base_delay_ms: 1_000,
            backoff_multiplier: 3,
        };

        let world = rebuild("orch-1", &issues, &history, None, &backoff);

        assert_eq!(
            world.backoff_until.get("T1").map(String::as_str),
            Some("2026-01-01T00:00:06Z"),
            "三次触发都停在首档 1s（不乘 3、不循环）"
        );
    }

    #[test]
    fn a_self_dependency_is_a_cycle_too() {
        let mut selfish = issue("A", IssueState::Open, None);
        selfish.blocked_by = vec!["A".to_string()];
        let issues = IssueListResponse {
            issues: vec![selfish],
        };
        let history = EventHistoryResponse {
            events: Vec::new(),
            truncated: false,
        };

        let world = rebuild("orch-1", &issues, &history, None, &RetryBackoff::default());

        assert_eq!(
            blocked_by_cycles(&issues),
            vec![vec!["A".to_string()]],
            "自环也是环"
        );
        assert_eq!(world, WorldModel::default());
    }

    #[test]
    fn an_unexpected_timestamp_shape_does_not_panic_and_skips_the_backoff_arithmetic() {
        // 宿主写入的是秒级 UTC（`...Z`）；遇到别的形状（带偏移量等）时不猜延迟、不 panic。
        let issues = IssueListResponse {
            issues: vec![issue("T1", IssueState::InProgress, Some("orch-1"))],
        };
        let history = EventHistoryResponse {
            events: vec![
                for_ticket(
                    event("e1", EventKind::Dispatch, "2026-01-01T08:00:00+08:00"),
                    "T1",
                    Some("a1"),
                ),
                for_ticket(
                    event("e2", EventKind::Interrupted, "2026-01-01T08:00:01+08:00"),
                    "T1",
                    Some("a1"),
                ),
            ],
            truncated: false,
        };
        let backoff = RetryBackoff {
            max_retries: 3,
            base_delay_ms: 1_000,
            backoff_multiplier: 2,
        };

        let world = rebuild("orch-1", &issues, &history, None, &backoff);

        assert_eq!(
            world.backoff_until.get("T1").map(String::as_str),
            Some("2026-01-01T08:00:01+08:00"),
            "认不出的形状就记触发时刻本身"
        );
        assert_eq!(
            world.event_watermark.as_deref(),
            Some("2026-01-01T08:00:01+08:00")
        );
    }
}

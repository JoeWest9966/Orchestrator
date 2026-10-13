//! `tick` 的验收测试：frontier 边界用例（全部堵塞 / 部分完成 / 环 / 他人已 claim / 冷却中）、
//! 重放确定性、零 IO。
//!
//! 期望值全部是票面文字或写死的字面量，不按实现的算法重算一遍。

use farm_sdk::services::tracker::IssueView;
use farm_sdk::workflow::{LastKnownGoodInfo, ProjectionSwitches, RetryBackoff};

use super::*;

/// claim 身份（`FARM_ASSIGNEE`）。
const ME: &str = "orch-1";
/// 别的编排器。
const OTHER: &str = "orch-2";
/// 本 tick 的钟面（宿主事件时间戳形状）。
const NOW: &str = "2026-01-01T00:00:00Z";
/// 票面 Fog 的处置：接入真实仓库前，e2e 用 `dispatch` 单标签（`60-environment-and-risks.md` §6.4）。
const DISPATCH_LABEL: &str = "dispatch";

fn issue(issue_id: &str, state: IssueState) -> IssueView {
    IssueView {
        issue_id: issue_id.to_string(),
        title: issue_id.to_string(),
        state,
        labels: vec![DISPATCH_LABEL.to_string()],
        assignee: None,
        updated_at: None,
        blocked_by: Vec::new(),
        blocking: Vec::new(),
    }
}

fn blocked_by(mut issue: IssueView, blockers: &[&str]) -> IssueView {
    issue.blocked_by = blockers.iter().map(|id| (*id).to_string()).collect();
    issue
}

fn assigned_to(mut issue: IssueView, assignee: &str) -> IssueView {
    issue.assignee = Some(assignee.to_string());
    issue
}

fn labelled(mut issue: IssueView, labels: &[&str]) -> IssueView {
    issue.labels = labels.iter().map(|label| (*label).to_string()).collect();
    issue
}

fn snapshot(issues: Vec<IssueView>) -> IssueListResponse {
    IssueListResponse { issues }
}

fn workflow() -> WorkflowConfig {
    WorkflowConfig {
        dispatch_labels: vec![DISPATCH_LABEL.to_string()],
        max_concurrency: 4,
        stall_threshold_secs: 300,
        prompt_template: "ticket {{ticket_id}}".to_string(),
        retry_backoff: RetryBackoff {
            max_retries: 3,
            base_delay_ms: 30_000,
            backoff_multiplier: 2,
        },
        projection: ProjectionSwitches::default(),
    }
}

fn config() -> EffectiveConfig {
    EffectiveConfig {
        effective: Some(workflow()),
        source: Some(EffectiveSource::Fresh),
    }
}

fn claim(ticket_id: &str) -> Decision {
    Decision::Claim {
        ticket_id: ticket_id.to_string(),
    }
}

fn dispatch(ticket_id: &str) -> Decision {
    Decision::Dispatch {
        ticket_id: ticket_id.to_string(),
    }
}

fn release(ticket_id: &str) -> Decision {
    Decision::Release {
        ticket_id: ticket_id.to_string(),
    }
}

// --- frontier 边界用例（ticket #3 验收第 2 条） ---

#[test]
fn an_all_blocked_frontier_yields_no_decisions() {
    // 「全部堵塞」：链头自己就被堵着（`blocked` 不是 `done`），整条链都算不出就绪。
    let issues = snapshot(vec![
        blocked_by(issue("T1", IssueState::Open), &["T2"]),
        blocked_by(issue("T2", IssueState::Open), &["T3"]),
        issue("T3", IssueState::Blocked),
    ]);

    assert_eq!(
        decide(&WorldModel::default(), &issues, &config(), NOW),
        Vec::new(),
        "没有就绪的票 ⇒ 决策表为空（票面的「空」）"
    );
}

#[test]
fn a_partially_done_blocker_set_leaves_the_ticket_blocked() {
    // 「部分完成」：C 的两张堵票里 A 完成了、B 没有 ⇒ C 还没就绪；B 自己是就绪的。
    let issues = snapshot(vec![
        issue("A", IssueState::Done),
        issue("B", IssueState::Open),
        blocked_by(issue("C", IssueState::Open), &["A", "B"]),
    ]);

    assert_eq!(
        decide(&WorldModel::default(), &issues, &config(), NOW),
        vec![claim("B")],
        "只抢 B；C 的 `blocked_by` 没全 done"
    );
}

#[test]
fn a_blocked_by_cycle_never_becomes_ready() {
    // 「环」：A↔B 成环，D 被环上的票堵着。环里的票都算不出就绪——判定不挂死、不 panic。
    let issues = snapshot(vec![
        blocked_by(issue("A", IssueState::Open), &["B"]),
        blocked_by(issue("B", IssueState::Open), &["A"]),
        blocked_by(issue("D", IssueState::Open), &["A"]),
        issue("F", IssueState::Open),
    ]);

    assert_eq!(
        decide(&WorldModel::default(), &issues, &config(), NOW),
        vec![claim("F")],
        "环上的票与被环堵住的票都不就绪；环外不受影响"
    );
}

#[test]
fn a_self_dependency_is_never_ready() {
    // 环的退化形：自环。
    let issues = snapshot(vec![blocked_by(issue("A", IssueState::Open), &["A"])]);

    assert_eq!(
        decide(&WorldModel::default(), &issues, &config(), NOW),
        Vec::new()
    );
}

#[test]
fn a_ticket_claimed_by_someone_else_is_not_in_the_frontier() {
    // 「他人已 claim」：归一化状态可能是 `claimed`（宿主侧 claim），也可能仍是 `open`
    // （provider 侧指派）；票面要求「排除已被**任何人** claim 的票」，所以两种都不进 frontier。
    let issues = snapshot(vec![
        assigned_to(issue("T1", IssueState::Claimed), OTHER),
        assigned_to(issue("T2", IssueState::Open), OTHER),
        assigned_to(issue("T3", IssueState::Open), ME),
    ]);

    assert_eq!(
        decide(&WorldModel::default(), &issues, &config(), NOW),
        Vec::new(),
        "别人的 claim 不抢；自己的 claim 也不在 frontier 里（它是派发的候选，不是抢票的）"
    );
}

#[test]
fn a_frontier_ticket_in_cooldown_is_not_claimed() {
    // 「冷却中」：票在 frontier 里，但冷却截止时刻还没到 ⇒ 本 tick 不碰它。
    let issues = snapshot(vec![issue("T1", IssueState::Open)]);
    let mut world = WorldModel::default();
    world
        .backoff_until
        .insert("T1".to_string(), "2026-01-01T00:00:30Z".to_string());

    assert_eq!(
        decide(&world, &issues, &config(), "2026-01-01T00:00:29Z"),
        Vec::new(),
        "抢来只会占着不动，而等待的是同一个冷却"
    );
}

#[test]
fn a_frontier_ticket_whose_cooldown_has_expired_is_claimed() {
    let issues = snapshot(vec![issue("T1", IssueState::Open)]);
    let mut world = WorldModel::default();
    world
        .backoff_until
        .insert("T1".to_string(), "2026-01-01T00:00:30Z".to_string());

    assert_eq!(
        decide(&world, &issues, &config(), "2026-01-01T00:00:30Z"),
        vec![claim("T1")],
        "截止时刻到了就不是冷却中"
    );
}

#[test]
fn the_frontier_label_must_intersect_the_effective_dispatch_labels() {
    let issues = snapshot(vec![
        labelled(issue("T1", IssueState::Open), &["bug"]),
        labelled(issue("T2", IssueState::Open), &["bug", DISPATCH_LABEL]),
    ]);

    assert_eq!(
        decide(&WorldModel::default(), &issues, &config(), NOW),
        vec![claim("T2")],
        "`labels ∩ dispatch_labels ≠ ∅` 至少命中一个"
    );
}

#[test]
fn a_blocked_by_id_outside_the_snapshot_is_not_done() {
    // 快照里查不到的堵票：推不出它完成了就不猜（也不把它当成无依赖的节点）。
    let issues = snapshot(vec![blocked_by(issue("T1", IssueState::Open), &["T9"])]);

    assert_eq!(
        decide(&WorldModel::default(), &issues, &config(), NOW),
        Vec::new()
    );
}

#[test]
fn a_done_blocker_releases_the_dependent() {
    let issues = snapshot(vec![
        issue("A", IssueState::Done),
        blocked_by(issue("C", IssueState::Open), &["A"]),
    ]);

    assert_eq!(
        decide(&WorldModel::default(), &issues, &config(), NOW),
        vec![claim("C")],
        "堵票全 done ⇒ 就绪"
    );
}

// --- dispatchable：配额与冷却 ---

#[test]
fn a_ticket_i_hold_without_an_active_attempt_is_dispatched_after_its_cooldown() {
    // 崩溃重启续跑：宿主重启把 attempt 标成 `interrupted`（ticket #2 场景），冷却过去之后我
    // 仍持有 claim、又没有活跃 attempt ⇒ 重新派发。
    let issues = snapshot(vec![assigned_to(issue("T1", IssueState::Claimed), ME)]);
    let mut world = WorldModel::default();
    world.claimed_by_me.insert("T1".to_string());
    world
        .backoff_until
        .insert("T1".to_string(), "2026-01-01T00:00:30Z".to_string());

    assert_eq!(
        decide(&world, &issues, &config(), "2026-01-01T00:00:31Z"),
        vec![dispatch("T1")]
    );
}

#[test]
fn a_ticket_with_an_active_attempt_is_neither_dispatched_nor_released() {
    let issues = snapshot(vec![assigned_to(issue("T1", IssueState::Claimed), ME)]);
    let mut world = WorldModel::default();
    world.claimed_by_me.insert("T1".to_string());
    world
        .active_attempts
        .insert("T1".to_string(), Some("a1".to_string()));

    assert_eq!(
        decide(&world, &issues, &config(), NOW),
        Vec::new(),
        "正在跑的票不重复派发"
    );

    world.merge_done.insert("T1".to_string());
    assert_eq!(
        decide(&world, &issues, &config(), NOW),
        Vec::new(),
        "活跃 attempt 还在跑的票绝不交还——那是把正在干活的票扔给下一个抢的人"
    );
}

#[test]
fn a_finished_ticket_i_hold_is_released() {
    let issues = snapshot(vec![assigned_to(issue("T1", IssueState::Done), ME)]);
    let mut world = WorldModel::default();
    world.claimed_by_me.insert("T1".to_string());

    assert_eq!(
        decide(&world, &issues, &config(), NOW),
        vec![release("T1")],
        "完成信号的排他所有权没有用途"
    );
}

#[test]
fn a_merge_visible_only_in_the_events_is_released_too() {
    // 投影开关默认全关，所以 merge 之后票可能仍是 `claimed`：完成信号由事件的 `merge_done`
    // 承载（ticket #2），release 认它。
    assert!(
        !workflow().projection.close_issue,
        "前提：默认投影开关不关票"
    );
    let issues = snapshot(vec![assigned_to(issue("T1", IssueState::Claimed), ME)]);
    let mut world = WorldModel::default();
    world.claimed_by_me.insert("T1".to_string());
    world.merge_done.insert("T1".to_string());

    assert_eq!(decide(&world, &issues, &config(), NOW), vec![release("T1")]);
}

#[test]
fn the_concurrency_quota_caps_the_claims_of_one_tick() {
    let issues = snapshot(vec![
        issue("T1", IssueState::Open),
        issue("T2", IssueState::Open),
        issue("T3", IssueState::Open),
        issue("T4", IssueState::Open),
        issue("T5", IssueState::Open),
    ]);
    let mut config = config();
    config
        .effective
        .as_mut()
        .expect("夹具带生效配置")
        .max_concurrency = 2;

    assert_eq!(
        decide(&WorldModel::default(), &issues, &config, NOW),
        vec![claim("T1"), claim("T2")],
        "配额 2 ⇒ 只抢两张，且按票 id 升序"
    );
}

#[test]
fn the_concurrency_quota_is_shared_by_dispatch_and_claim() {
    let issues = snapshot(vec![
        assigned_to(issue("M", IssueState::Claimed), ME),
        issue("F1", IssueState::Open),
        issue("F2", IssueState::Open),
    ]);
    let mut world = WorldModel::default();
    world.claimed_by_me.insert("M".to_string());
    world
        .active_attempts
        .insert("R".to_string(), Some("a1".to_string()));

    let mut tight = config();
    tight
        .effective
        .as_mut()
        .expect("夹具带生效配置")
        .max_concurrency = 2;
    assert_eq!(
        decide(&world, &issues, &tight, NOW),
        vec![dispatch("M")],
        "1 个槽被活跃 attempt 占着，只剩 1 个：已持有的票先派发，余量为零就不抢票"
    );

    let mut roomy = config();
    roomy
        .effective
        .as_mut()
        .expect("夹具带生效配置")
        .max_concurrency = 3;
    assert_eq!(
        decide(&world, &issues, &roomy, NOW),
        vec![dispatch("M"), claim("F1")],
        "2 个余量：派发 + 抢票各吃一个"
    );
}

#[test]
fn claims_taken_in_one_tick_are_dispatched_within_the_quota_in_the_next() {
    // 抢票也吃配额的理由：只按 `active` 判，一个 tick 会把整个 frontier 抢光，下个 tick 再
    // 一次性派发，超出 max_concurrency。
    let issues = snapshot(
        (1..=5)
            .map(|n| issue(&format!("T{n}"), IssueState::Open))
            .collect(),
    );
    let mut config = config();
    config
        .effective
        .as_mut()
        .expect("夹具带生效配置")
        .max_concurrency = 2;

    let tick_one = decide(&WorldModel::default(), &issues, &config, NOW);
    assert_eq!(tick_one, vec![claim("T1"), claim("T2")]);

    // 执行器打完 claim、对账重建之后的世界：两张票归我，还没有活跃 attempt。
    let mut world = WorldModel::default();
    for decision in &tick_one {
        if let Decision::Claim { ticket_id } = decision {
            world.claimed_by_me.insert(ticket_id.clone());
        }
    }

    assert_eq!(
        decide(&world, &issues, &config, NOW),
        vec![dispatch("T1"), dispatch("T2")],
        "下个 tick 在飞的派发数 = 配额，不超"
    );
}

// --- ADR-0004 三态 ---

#[test]
fn an_absent_effective_config_yields_no_decisions() {
    let issues = snapshot(vec![issue("T1", IssueState::Open)]);
    let refused = EffectiveConfig {
        effective: None,
        source: None,
    };

    assert_eq!(
        decide(&WorldModel::default(), &issues, &refused, NOW),
        Vec::new(),
        "尚无生效配置 ⇒ idle：不 claim、不 dispatch，不得自造默认配置"
    );
    assert_eq!(
        decide(&WorldModel::default(), &issues, &config(), NOW),
        vec![claim("T1")],
        "同一份快照在生效配置可用时是有决策的——证明上一条不是空断言"
    );
}

#[test]
fn a_last_known_good_source_orchestrates_normally() {
    // ADR-0004：LKG 照常编排。「持续标注」是 tick 循环（#5）的活——它要写日志，而本模块零 IO，
    // 所以 `source` 只被带过、不被分支。
    let issues = snapshot(vec![issue("T1", IssueState::Open)]);
    let mut lkg = config();
    lkg.source = Some(EffectiveSource::LastKnownGood);

    assert_eq!(
        decide(&WorldModel::default(), &issues, &lkg, NOW),
        vec![claim("T1")]
    );
}

#[test]
fn the_effective_config_is_derived_from_the_admin_status() {
    let fresh = WorkflowConfigStatus {
        repo_id: "repo-1".to_string(),
        effective: Some(workflow()),
        source: Some(EffectiveSource::Fresh),
        loaded_at: Some(NOW.to_string()),
        last_known_good: None,
        last_alert: None,
    };
    assert_eq!(
        EffectiveConfig::from_status(&fresh),
        EffectiveConfig {
            effective: Some(workflow()),
            source: Some(EffectiveSource::Fresh),
        }
    );

    let refused = WorkflowConfigStatus {
        repo_id: "repo-1".to_string(),
        effective: None,
        source: None,
        loaded_at: None,
        last_known_good: Some(LastKnownGoodInfo {
            updated_at: "2025-12-31T00:00:00Z".to_string(),
        }),
        last_alert: Some("invalid workflow config; refusing to orchestrate".to_string()),
    };
    assert_eq!(
        EffectiveConfig::from_status(&refused).effective,
        None,
        "`effective == None` 就是拒绝编排，不因为别的字段看着有东西就造一份配置"
    );
}

// --- 重放确定性（ticket #3 验收第 1 条） ---

#[test]
fn the_replay_of_a_shuffled_world_produces_the_same_decisions() {
    // 固定种子的 xorshift 打乱输入快照的顺序（以及 world 里集合的插入顺序），决策表必须逐字
    // 不变。不用钟面随机：重放单测自己得是可重放的。
    let issues = replay_snapshot();
    assert_eq!(
        decide(&replay_world(&mut Shuffle(1)), &issues, &config(), NOW),
        replay_expected(),
        "先钉住字面期望值（票面文字推导），再谈打乱"
    );

    for seed in 1..=8u64 {
        let mut shuffle = Shuffle(seed);
        let mut shuffled = issues.issues.clone();
        shuffle.permute(&mut shuffled);
        assert_eq!(shuffled.len(), issues.issues.len(), "打乱保持元素个数");

        assert_eq!(
            decide(
                &replay_world(&mut shuffle),
                &IssueListResponse { issues: shuffled },
                &config(),
                NOW
            ),
            replay_expected(),
            "seed {seed}：决策表与输入顺序无关"
        );
    }
}

/// 重放夹具的期望决策表（写死，不按实现重算）：
///
/// * `D0`：我持有 + `merge_done`（完成信号）+ 无活跃 attempt ⇒ 交还；
/// * `M`：我持有 + 无活跃 attempt + 未完成 + 不在冷却中 ⇒ 派发；
/// * `F1`：就绪 + 不在冷却中 ⇒ 抢票（余量：`max_concurrency = 3` 减 1 个活跃 attempt = 2）；
/// * `R`（活跃 attempt）、`F2`（冷却中）、`C1`/`C2`（环）、`O1`（他人持有）、`L1`（标签不匹配）、
///   `X1`（堵票不在快照里）一律无决策。
fn replay_expected() -> Vec<Decision> {
    vec![release("D0"), dispatch("M"), claim("F1")]
}

fn replay_snapshot() -> IssueListResponse {
    snapshot(vec![
        assigned_to(issue("D0", IssueState::Claimed), ME),
        assigned_to(issue("M", IssueState::Claimed), ME),
        assigned_to(issue("R", IssueState::Claimed), ME),
        issue("F1", IssueState::Open),
        issue("F2", IssueState::Open),
        blocked_by(issue("C1", IssueState::Open), &["C2"]),
        blocked_by(issue("C2", IssueState::Open), &["C1"]),
        assigned_to(issue("O1", IssueState::Open), OTHER),
        labelled(issue("L1", IssueState::Open), &["bug"]),
        blocked_by(issue("X1", IssueState::Open), &["T9"]),
    ])
}

/// 用与快照同一份打乱顺序填 world 的集合。
///
/// `claimed_by_me` / `merge_done` / `backoff_until` 是 `BTree*`，插入顺序对它们不可见——
/// 本函数把这一点**测**出来（每个 seed 换一个插入次序），而不是假定它。
fn replay_world(shuffle: &mut Shuffle) -> WorldModel {
    let strings =
        |ids: &[&str]| -> Vec<String> { ids.iter().map(|id| (*id).to_string()).collect() };

    let mut claimed = strings(&["D0", "M", "R"]);
    shuffle.permute(&mut claimed);
    let mut merged = strings(&["D0"]);
    shuffle.permute(&mut merged);
    let mut cooling = strings(&["F2"]);
    shuffle.permute(&mut cooling);

    let mut world = WorldModel::default();
    world
        .active_attempts
        .insert("R".to_string(), Some("a1".to_string()));
    for ticket_id in claimed {
        world.claimed_by_me.insert(ticket_id);
    }
    for ticket_id in merged {
        world.merge_done.insert(ticket_id);
    }
    for ticket_id in cooling {
        world
            .backoff_until
            .insert(ticket_id, "2026-01-01T00:01:00Z".to_string());
    }
    world
}

/// 固定种子的 xorshift64：重放单测只需要**确定**的打乱。
struct Shuffle(u64);

impl Shuffle {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Fisher–Yates：就地打乱。
    fn permute<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = (self.next() % (i as u64 + 1)) as usize;
            items.swap(i, j);
        }
    }
}

// --- 零 IO（ticket #3 验收第 3 条） ---

#[test]
fn decide_is_a_plain_synchronous_function() {
    // 决策面可以整体当成一个 `fn` 指针传出去：没有 `self`（无状态）、没有可等待体、
    // 没有 `Result`（没有 IO 错误面）——这是类型层的零 IO 证明。
    let seam: fn(&WorldModel, &IssueListResponse, &EffectiveConfig, &str) -> Vec<Decision> = decide;
    let issues = snapshot(vec![issue("T1", IssueState::Open)]);

    assert_eq!(
        seam(&WorldModel::default(), &issues, &config(), NOW),
        vec![claim("T1")]
    );
}

#[test]
fn the_decide_module_has_no_clock_and_no_io() {
    // 源码层的零 IO 证明：模块里既没有钟面也没有 IO 面，依赖也越不出这三条。
    let source = include_str!("mod.rs");
    for forbidden in [
        "SystemTime",
        "Instant",
        "std::time",
        "std::io",
        "std::fs",
        "std::net",
        "std::process",
        "std::env",
        "std::thread",
        "tokio",
        "async",
        "await",
        "TcpStream",
        "reqwest",
    ] {
        assert!(
            !source.contains(forbidden),
            "decide 模块不得出现 {forbidden}——时间只能从 `now` 参数进，IO 只能由调用方做"
        );
    }

    let allowed = ["use std::collections", "use farm_sdk", "use crate::recon"];
    for line in source.lines().filter(|line| line.starts_with("use ")) {
        assert!(
            allowed.iter().any(|prefix| line.starts_with(prefix)),
            "越界的依赖：{line}"
        );
    }
}

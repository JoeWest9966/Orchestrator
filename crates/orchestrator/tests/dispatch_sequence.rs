//! 端到端：一张票从 claim 走到 dispatch 成功（票 #4 验收第 2 条）。
//!
//! 夹具复用 skeleton 的形状：**真实 farm-host 进程**、**fake-pi**（`FAKE_PI_SCENARIO=normal`）、
//! tempdir 隔离的宿主库、一个真实的 git 仓库。全程编排器只是契约客户端（只过 farm-sdk 类型
//! 与契约 / 管理 HTTP API，无私有后门），**观测也全部走契约**：归一化视图、workspace 映射、
//! 事件历史。
//!
//! 找不到宿主或 fake-pi 就 **panic**（不是跳过）：这条演练是票 #4 的验收标准，零断言的绿不是绿
//! （`common::require_host_and_fake_pi`）；跑起来之后用「防空洞」断言检查条数，
//! 保证没有半途而废也能变绿。

mod common;

use std::path::Path;

use farm_sdk::events::EventKind;
use farm_sdk::handshake::CONTRACT_VERSION;
use farm_sdk::services::events::EventHistoryQuery;
use farm_sdk::services::tracker::IssueState;
use farm_sdk::services::workspace::{WorktreeEstablishRequest, WorktreeState};
use orchestrator::exec::{dispatch_ticket, Outcome, WorktreeLayout};

use common::{require_host_and_fake_pi, scaffold_repo, Checks, TestHost, INTEGRATION, SPEC};

/// 被派发的那张票（宿主播种的原生票号即票 id）。
const TICKET: &str = "7";
const TICKET_NUMBER: u32 = 7;
/// 夹具前置里「早先那张票」：它的映射给出本编排器的 spec / 集成分支。
const SEED_TICKET: &str = "101";
const SEED_TICKET_NUMBER: u32 = 101;
/// claim 身份（`FARM_ASSIGNEE`）。
const ASSIGNEE: &str = "orchestrator-e2e";
/// 已生效的 workflow 配置：宿主在建立 worktree 时从 `.farm/workflow.json` 装载它。
const WORKTREE_BRANCH: &str = "farm/t7";
/// 已生效的 workflow 配置（宿主在建立 worktree 时从 `.farm/workflow.json` 装载它）。
///
/// `projection.close_issue = true`：merge 之后宿主把票机械标成 `done`（完成信号的投影）。
const WORKFLOW_JSON: &str = r#"{"dispatch_labels":["dispatch"],"max_concurrency":1,"stall_threshold_secs":3600,"prompt_template":"work on {{ticket_id}}","retry_backoff":{"max_retries":2,"base_delay_ms":1000,"backoff_multiplier":2},"projection":{"close_issue":true,"open_pr":false,"comment_merge_commit":false,"add_label":false}}"#;

/// 机械断言的下限（防空洞）。
const MIN_CHECKS: usize = 10;

#[tokio::test]
async fn one_ticket_goes_from_claim_to_a_successful_dispatch_against_a_real_host() {
    let (_host_bin, _fake_pi) = require_host_and_fake_pi();

    let scratch = tempfile::tempdir().expect("scratch dir");
    let repo = scratch.path().join("repo");
    scaffold_repo(&repo, WORKFLOW_JSON);
    let worktrees = scratch.path().join("worktrees");

    let host = TestHost::spawn(
        &[TICKET_NUMBER, SEED_TICKET_NUMBER],
        &[
            (
                "FARM_AGENT_PROGRAM",
                _fake_pi.to_string_lossy().into_owned(),
            ),
            ("FAKE_PI_SCENARIO", "normal".to_string()),
        ],
    )
    .await;
    let client = host.client();
    let mut checks = Checks::default();
    // 前置：两个替身二进制都真的到场（前置不齐就在这里失败，而不是伪装成绿）。
    checks.record(
        "the drill prerequisites are present (a real farm-host and the fake agent)",
        true,
        format!("{_host_bin:?} + {_fake_pi:?}"),
    );

    // 夹具就绪：契约面可达（握手）。
    match client.handshake().await {
        Ok(response) => checks.record(
            "the contract face answers the handshake",
            response.contract_version == CONTRACT_VERSION,
            format!("contract_version {}", response.contract_version),
        ),
        Err(error) => checks.record(
            "the contract face answers the handshake",
            false,
            error.to_string(),
        ),
    }

    // 夹具前置：workspace 映射（spec ↔ 集成分支）必须先存在。派发序列的 `spec_id` /
    // `integration_branch` 只能从 `GET /v1/workspace/mappings` 读（见 `exec` 模块头的
    // `[UNSOURCED]`），而 mappings 只能由 establish 原语产生——这里用**同一个契约原语**为一张
    // 早先的票建一条映射，模拟「本编排器之前已经在这个 spec 上跑过」。这条前置是环境缺口，
    // 不是编排器能自己解决的事。
    let seed = WorktreeEstablishRequest {
        spec_id: SPEC.to_string(),
        integration_branch: INTEGRATION.to_string(),
        ticket_id: SEED_TICKET.to_string(),
        repo_path: repo.to_string_lossy().into_owned(),
        worktree_path: worktrees.join("seed").to_string_lossy().into_owned(),
        worktree_branch: "farm/seed".to_string(),
        start_point: Some(INTEGRATION.to_string()),
    };
    match client.establish_worktree(&seed).await {
        Ok(worktree) => checks.record(
            "the spec mapping exists before the sequence runs (bootstrap)",
            worktree.state == WorktreeState::Ready,
            format!("seed worktree {} on {}", worktree.path, worktree.branch),
        ),
        Err(error) => checks.record(
            "the spec mapping exists before the sequence runs (bootstrap)",
            false,
            error.to_string(),
        ),
    }

    // 生效配置读**管理面**（独立 admin token）：`prompt_template` 与 `retry_backoff` 都从这里来。
    let workflow =
        match client.workflow_config(SPEC).await {
            Ok(status) => {
                checks.record(
                "the admin face reports the effective workflow config (independent admin token)",
                status.effective.is_some(),
                format!("source {:?}, loaded_at {:?}", status.source, status.loaded_at),
            );
                status.effective
            }
            Err(error) => {
                checks.record(
                "the admin face reports the effective workflow config (independent admin token)",
                false,
                error.to_string(),
            );
                None
            }
        };

    // 三条 `[UNSOURCED]` 路径由调用方供给（exec 模块头：不猜 env 名、不猜路径约定）。
    let layout = WorktreeLayout {
        repo_path: repo.to_string_lossy().into_owned(),
        worktree_path: worktrees.join("t7").to_string_lossy().into_owned(),
        worktree_branch: WORKTREE_BRANCH.to_string(),
    };

    let Some(workflow) = workflow else {
        assert!(checks.all_ok(), "e2e 前置失败：\n{}", checks.render());
        return;
    };

    // ===== 被测：一张票的派发序列 =====
    let outcome = dispatch_ticket(&client, ASSIGNEE, TICKET, &workflow, &layout).await;
    let attempt_id = match &outcome {
        Outcome::Dispatched { attempt_id } => Some(attempt_id.clone()),
        _ => None,
    };
    checks.record(
        "the dispatch sequence takes the ticket from claim to a successful dispatch",
        attempt_id.is_some(),
        format!("outcome {outcome:?}"),
    );

    // 观测一：归一化视图说这张票在本 assignee 名下（claim 的权威投影就是视图本身）。
    match client.issues().await {
        Ok(issues) => {
            let ticket = issues.issues.iter().find(|issue| issue.issue_id == TICKET);
            checks.record(
                "the normalized view reports the ticket as claimed by this assignee",
                ticket.and_then(|ticket| ticket.assignee.as_deref()) == Some(ASSIGNEE)
                    && ticket.map(|ticket| ticket.state) == Some(IssueState::Claimed),
                format!("{ticket:?}"),
            );
        }
        Err(error) => checks.record(
            "the normalized view reports the ticket as claimed by this assignee",
            false,
            error.to_string(),
        ),
    }

    // 观测二：workspace 映射里有本票的 worktree，而且它真的落在磁盘上。
    match client.mappings().await {
        Ok(mappings) => {
            let mapped = mappings
                .mappings
                .iter()
                .flat_map(|mapping| mapping.worktrees.iter())
                .find(|worktree| worktree.ticket_id == TICKET);
            checks.record(
                "the workspace service built the ticket worktree (mapped ready)",
                mapped.map(|worktree| worktree.state) == Some(WorktreeState::Ready)
                    && mapped.map(|worktree| worktree.path.as_str())
                        == Some(layout.worktree_path.as_str())
                    && mapped.map(|worktree| worktree.branch.as_str()) == Some(WORKTREE_BRANCH),
                format!("{mapped:?}"),
            );
            checks.record(
                "the worktree on disk is a real git worktree",
                Path::new(&layout.worktree_path).join(".git").exists(),
                layout.worktree_path.to_string(),
            );
        }
        Err(error) => checks.record(
            "the workspace service built the ticket worktree (mapped ready)",
            false,
            error.to_string(),
        ),
    }

    // 观测三：事件历史——claim 是一等事件，dispatch 带上本次 attempt id。
    let history = client
        .event_history(&EventHistoryQuery {
            ticket_id: Some(TICKET.to_string()),
            ..Default::default()
        })
        .await;
    match history {
        Ok(history) => {
            let claims = history
                .events
                .iter()
                .filter(|event| event.kind == EventKind::ClaimAcquired)
                .count();
            checks.record(
                "the claim is a first-class event, and the idempotent re-claim adds no second one",
                claims == 1,
                format!(
                    "{claims} claim_acquired event(s) for {TICKET} (the sequence claims twice)"
                ),
            );
            let dispatch_event = history
                .events
                .iter()
                .find(|event| event.kind == EventKind::Dispatch);
            checks.record(
                "the dispatch lands on the event bus with the returned attempt id",
                dispatch_event.and_then(|event| event.attempt_id.as_deref())
                    == attempt_id.as_deref(),
                format!(
                    "event attempt_id {:?}",
                    dispatch_event.and_then(|e| e.attempt_id.clone())
                ),
            );
            checks.record(
                "the dispatch request carries no invented profile",
                dispatch_event.map(|event| {
                    event
                        .payload
                        .get("profile")
                        .is_none_or(serde_json::Value::is_null)
                }) == Some(true),
                format!(
                    "payload.profile {:?}",
                    dispatch_event.and_then(|event| event.payload.get("profile").cloned())
                ),
            );
        }
        Err(error) => checks.record(
            "the dispatch lands on the event bus with the returned attempt id",
            false,
            error.to_string(),
        ),
    }

    assert!(checks.all_ok(), "端到端没跑绿：\n{}", checks.render());
    // 防空洞：跑起来之后至少要有这么多条机械断言，否则「静默半途而废」会伪装成绿。
    assert!(
        checks.len() >= MIN_CHECKS,
        "防空洞：至少 {MIN_CHECKS} 条机械断言，实际 {}",
        checks.len()
    );
}

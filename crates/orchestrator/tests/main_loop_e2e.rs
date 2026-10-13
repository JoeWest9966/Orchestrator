//! 端到端演练：**真宿主** + **真编排器二进制**（spawn 成子进程）+ fake-pi（票 #5 验收）。
//!
//! 三条演练（票 #5 验收三条逐一对应一个 `#[tokio::test]`）：
//!
//! 1. `one_ticket_goes_from_open_to_a_merge_event`：票 `open` → `merge` 事件出现，全程编排器
//!    只是契约客户端。**测试扮演 agent**：在票 worktree 里做出工作提交（路径从
//!    `GET /v1/workspace/mappings` 读——契约读，不是 env 约定），再用契约 HTTP 提交
//!    `MergeProposal`。提议**不在**编排器范围里（`POST /v1/merge/proposals` 是 agent 的活），
//!    宿主机机械校验（worktree 干净 / FF 可行 / 测试过）后执行 merge，产生 `merge` 事件；
//!    编排器只**观测**它。
//! 2. `a_sigkill_then_a_restart_does_not_dispatch_twice`：编排器被 `SIGKILL` 之后重启，
//!    **无双派**。可观察量：那张票在两个进程的生命期里**只有一条 `dispatch` 事件**
//!    （宿主的 claim/事件账在两次运行之间是同一份），且第二次运行确实在跑（日志里有 tick 行）。
//! 3. `a_corrupt_workflow_json_keeps_orchestrating_on_the_last_known_good`：非法
//!    `.farm/workflow.json` 下编排器继续用 LKG 编排**并持续标注**（断言编排器日志）。
//!
//! 夹具形状照 skeleton（`tests/common/mod.rs` 的 `TestHost`：真 `farm-host` + fake-pi +
//! tempdir）。找不到宿主 / fake-pi 就**跳过**（不 fail）；跑起来之后用「防空洞」断言检查
//! 条数，保证「静默半途而废」不能伪装成绿。

mod common;

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use farm_sdk::events::EventKind;
use farm_sdk::services::merge::MergeProposal;
use farm_sdk::services::tracker::IssueState;
use farm_sdk::services::workspace::{WorktreeEstablishRequest, WorktreeState};
use orchestrator::main_loop::{ENV_ASSIGNEE, ENV_HOST, ENV_WORKTREE_BRANCH, ENV_WORKTREE_PATH};

use common::{
    events_of_kind, fake_pi_bin, host_bin, poll, propose_merge, Checks, OrchestratorEnv,
    OrchestratorRun, TestHost, SEED_LABEL,
};

/// 被编排的那张票（宿主播种的原生票号即票 id）。
const TICKET: &str = "7";
const TICKET_NUMBER: u32 = 7;
/// 夹具前置的「早先那张票」：它的映射给出本编排器的 spec / 集成分支。
///
/// 它**不是**一张被播种的 issue（只在 workspace 映射里存在）：这样它不会挤进 frontier 抢
/// 并发配额——编排器要编排的只有 [`TICKET`]。
const SEED_TICKET: &str = "seed";
const SPEC: &str = "spec-10";
const INTEGRATION: &str = "implement/spec-10";

/// 已生效的 workflow 配置（宿主在建立 worktree 时从 `.farm/workflow.json` 装载它）。
///
/// `projection.close_issue = true`：merge 之后宿主把票机械标成 `done`（完成信号的投影）。
/// `stall_threshold_secs` 取大值：演练用 `silence` 场景，让 attempt 一直「在跑」。
fn workflow_json() -> String {
    format!(
        r#"{{"dispatch_labels":["{SEED_LABEL}"],"max_concurrency":1,"stall_threshold_secs":3600,"prompt_template":"work on {{{{ticket_id}}}}","retry_backoff":{{"max_retries":2,"base_delay_ms":1000,"backoff_multiplier":2}},"projection":{{"close_issue":true,"open_pr":false,"comment_merge_commit":false,"add_label":false}}}}"#
    )
}

/// scratch 仓库：`main` + 集成分支 + 仓库里的 `.farm/workflow.json`（宿主读它做生效配置）。
fn scaffold_repo(repo: &Path, workflow: &str) {
    std::fs::create_dir_all(repo).expect("create repo dir");
    git(repo, &["init", "--initial-branch=main"]);
    git(repo, &["config", "user.email", "orchestrator@farm.local"]);
    git(repo, &["config", "user.name", "Orchestrator E2E"]);
    std::fs::write(repo.join("README.md"), "main-loop e2e scratch\n").expect("readme");
    std::fs::create_dir_all(repo.join(".farm")).expect("mkdir .farm");
    std::fs::write(repo.join(".farm/workflow.json"), workflow).expect("workflow.json");
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);
    git(repo, &["branch", INTEGRATION]);
}

fn git(cwd: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .output()
        .expect("spawn git");
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// 写一个「测试过」闸门的脚本（`FARM_MERGE_TEST_COMMAND` 指向它）：退出 0 = 测试过。
fn passing_test_script(dir: &Path) -> String {
    let path = dir.join("merge-tests.sh");
    std::fs::write(&path, "#!/bin/sh\nexit 0\n").expect("write test script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&path).expect("stat").permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).expect("chmod");
    }
    path.to_string_lossy().into_owned()
}

/// 一条 tick 汇总行。
///
/// 为什么不数 `tick=`：`tracing` 的默认 fmt 会给**字段名与 `=`** 上色（实际字节是
/// `\x1b[3mtick\x1b[0m\x1b[2m=\x1b[0m`），所以 `tick=` 这个字面量在日志里**不存在**
/// （探针实证）。可靠可读的部分是**消息**（纯文本）与字段名文本：每 tick 的汇总行是
/// `... : tick tick=<n> source=... decisions=<n> ...`，其中 `decisions` 只在这一行上出现。
fn is_tick_line(line: &str) -> bool {
    line.contains(" tick ") && line.contains("decisions")
}

fn missing_binaries() -> bool {
    if host_bin().is_none() || fake_pi_bin().is_none() {
        eprintln!(
            "farm-host 或 fake-pi 二进制没找到（显式给 FARM_HOST_BIN / FAKE_PI_BIN；\
             CARGO_TARGET_DIR 是本 worktree 本地的 target，替身不在那里）；跳过 main_loop e2e"
        );
        return true;
    }
    false
}

/// 夹具前置：先建一条 spec ↔ 集成分支的映射（编排器的 `spec_id` / `integration_branch`
/// 只能从 `GET /v1/workspace/mappings` 读，而映射只能由 establish 原语产生）。
async fn bootstrap_mapping(
    client: &orchestrator::contract::client::ContractClient,
    repo: &Path,
    root: &Path,
) {
    let request = WorktreeEstablishRequest {
        spec_id: SPEC.to_string(),
        integration_branch: INTEGRATION.to_string(),
        ticket_id: SEED_TICKET.to_string(),
        repo_path: repo.to_string_lossy().into_owned(),
        worktree_path: root.join("seed").to_string_lossy().into_owned(),
        worktree_branch: "farm/seed".to_string(),
        start_point: Some(INTEGRATION.to_string()),
    };
    let worktree = client
        .establish_worktree(&request)
        .await
        .expect("bootstrap establish（夹具前置，不是编排器的能力）");
    assert_eq!(worktree.state, WorktreeState::Ready, "{worktree:?}");
}

/// 等到那张票的 worktree 出现在 workspace 映射里，返回它的路径与分支。
async fn wait_for_ticket_worktree(
    client: &orchestrator::contract::client::ContractClient,
    timeout: Duration,
) -> Option<(String, String)> {
    poll(timeout, || async {
        let mappings = client.mappings().await.ok()?;
        mappings
            .mappings
            .iter()
            .flat_map(|mapping| mapping.worktrees.iter())
            .find(|worktree| worktree.ticket_id == TICKET)
            .map(|worktree| (worktree.path.clone(), worktree.branch.clone()))
    })
    .await
}

// ---------------------------------------------------------------------------
// 演练 1：open → merge 事件
// ---------------------------------------------------------------------------

#[tokio::test]
async fn one_ticket_goes_from_open_to_a_merge_event() {
    if missing_binaries() {
        return;
    }
    let scratch = tempfile::tempdir().expect("scratch dir");
    let repo = scratch.path().join("repo");
    let worktrees = scratch.path().join("worktrees");
    scaffold_repo(&repo, &workflow_json());

    let host = TestHost::spawn(
        &[TICKET_NUMBER],
        &[
            (
                "FARM_AGENT_PROGRAM",
                fake_pi_bin().unwrap().to_string_lossy().into_owned(),
            ),
            // `silence`：attempt 一直「在跑」⇒ 编排器不会重复派发（无双派的可观察量从这条起就成立）。
            ("FAKE_PI_SCENARIO", "silence".to_string()),
            (
                "FARM_MERGE_TEST_COMMAND",
                passing_test_script(scratch.path()),
            ),
        ],
    )
    .await;
    let client = host.client();
    bootstrap_mapping(&client, &repo, &worktrees).await;

    let env = OrchestratorEnv::new(host.addr, &repo.to_string_lossy(), &worktrees);
    let mut orchestrator = OrchestratorRun::spawn(&env, scratch.path().join("orchestrator-1.log"));
    let mut checks = Checks::default();

    // ① 编排器把票从 open 推到「已派发」：dispatch 事件是它做出来的（别的角色都不会 dispatch）。
    let dispatched = poll(Duration::from_secs(90), || async {
        events_of_kind(&client, TICKET, EventKind::Dispatch)
            .await
            .first()
            .map(|event| event.attempt_id.clone().unwrap_or_default())
    })
    .await;
    checks.record(
        "the orchestrator dispatches the ticket (the dispatch event is its work)",
        dispatched.is_some(),
        format!("attempt_id {dispatched:?}"),
    );

    // ② 编排器真的只是契约客户端：它观察到的世界来自契约读，而 worktree 是它委托宿主建的。
    let worktree = wait_for_ticket_worktree(&client, Duration::from_secs(30)).await;
    checks.record(
        "the host built the ticket worktree the orchestrator asked for (contract only)",
        worktree
            .as_ref()
            .is_some_and(|(path, _)| Path::new(path).join(".git").exists()),
        format!("{worktree:?}"),
    );
    let Some((worktree_path, worktree_branch)) = worktree else {
        assert!(checks.all_ok(), "e2e 前置失败：\n{}", checks.render());
        return;
    };

    // ③ **测试扮演 agent**：在票 worktree 里做出工作提交，再按契约提交 merge 提议。
    std::fs::write(
        Path::new(&worktree_path).join("work.txt"),
        "ticket 7 work\n",
    )
    .expect("work");
    git(Path::new(&worktree_path), &["add", "-A"]);
    git(
        Path::new(&worktree_path),
        &["commit", "-m", "work: ticket 7"],
    );
    let proposal = MergeProposal {
        ticket_id: TICKET.to_string(),
        worktree_branch: worktree_branch.clone(),
        commit: None,
        message: Some("ticket 7".to_string()),
    };
    let proposed = propose_merge(host.addr, &env.token, &proposal).await;
    checks.record(
        "the agent (played by the test) proposes the merge through the contract",
        proposed.is_ok(),
        format!("{proposed:?}"),
    );

    // ④ 完成信号：集成分支上的 merge 事件（机械事实，不靠自述）。
    let merged = poll(Duration::from_secs(90), || async {
        events_of_kind(&client, TICKET, EventKind::Merge)
            .await
            .first()
            .cloned()
    })
    .await;
    checks.record(
        "the host executes the merge and the completion signal (merge event) appears",
        merged.is_some(),
        format!(
            "{:?}",
            merged.as_ref().map(|event| event.occurred_at.clone())
        ),
    );

    // ⑤ 编排器**观测到**完成信号：下一个 tick 交还排他所有权（视图里的 assignee 被清掉）。
    //    这是「merge 事件确实进了编排器的世界」的可观察量——编排器从不自己 merge。
    let released = poll(Duration::from_secs(60), || async {
        let issues = client.issues().await.ok()?;
        let ticket = issues
            .issues
            .iter()
            .find(|issue| issue.issue_id == TICKET)?
            .clone();
        (ticket.assignee.is_none()).then_some(ticket)
    })
    .await;
    checks.record(
        "the orchestrator observes the completion signal and releases the ticket it held",
        released.is_some(),
        format!("{released:?}"),
    );
    checks.record(
        "the completion projection closes the ticket (the host's mechanical projection)",
        released
            .as_ref()
            .is_some_and(|ticket| ticket.state == IssueState::Done),
        format!("{:?}", released.as_ref().map(|ticket| ticket.state)),
    );

    // ⑥ 全程只有一次派发：`silence` 场景下 attempt 一直在跑，世界里的活跃 attempt 阻止重派。
    let dispatches = events_of_kind(&client, TICKET, EventKind::Dispatch).await;
    checks.record(
        "the orchestrator observed the merge instead of proposing one (no second dispatch)",
        dispatches.len() == 1,
        format!("{} dispatch event(s)", dispatches.len()),
    );

    // ⑦ WS 订阅确实连上了（票 #5 的 WS 入账通路对着真宿主跑了一次）。
    let log = orchestrator.log();
    checks.record(
        "the long-lived event stream subscription came up against the real host",
        log.contains("event stream subscription established"),
        format!("log {} bytes", log.len()),
    );
    checks.record(
        "the orchestrator binary is still running (the loop did not die)",
        orchestrator.is_alive(),
        format!("pid {}", orchestrator.pid()),
    );

    // ⑧ 编排器用的 worktree 就是 env 模板对这一张票展开出来的那一条（模板机制端到端成立）。
    checks.record(
        "the worktree the host built is what the env template expands to for this ticket",
        worktree_path == worktrees.join(TICKET).to_string_lossy(),
        format!("{worktree_path} vs {}", worktrees.join(TICKET).display()),
    );

    assert!(checks.all_ok(), "端到端没跑绿：\n{}", checks.render());
    assert!(
        checks.len() >= 9,
        "防空洞：至少 9 条机械断言，实际 {}",
        checks.len()
    );
}

// ---------------------------------------------------------------------------
// 演练 2：SIGKILL → 重启 → 无双派
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_sigkill_then_a_restart_does_not_dispatch_twice() {
    if missing_binaries() {
        return;
    }
    let scratch = tempfile::tempdir().expect("scratch dir");
    let repo = scratch.path().join("repo");
    let worktrees = scratch.path().join("worktrees");
    scaffold_repo(&repo, &workflow_json());

    let host = TestHost::spawn(
        &[TICKET_NUMBER],
        &[
            (
                "FARM_AGENT_PROGRAM",
                fake_pi_bin().unwrap().to_string_lossy().into_owned(),
            ),
            ("FAKE_PI_SCENARIO", "silence".to_string()),
            (
                "FARM_MERGE_TEST_COMMAND",
                passing_test_script(scratch.path()),
            ),
        ],
    )
    .await;
    let client = host.client();
    bootstrap_mapping(&client, &repo, &worktrees).await;

    let env = OrchestratorEnv::new(host.addr, &repo.to_string_lossy(), &worktrees);
    let mut checks = Checks::default();

    // 第一次运行：等到那张票被派发（第一次派发由第一个进程做出来）。
    let mut first = OrchestratorRun::spawn(&env, scratch.path().join("orchestrator-1.log"));
    let dispatched = poll(Duration::from_secs(90), || async {
        events_of_kind(&client, TICKET, EventKind::Dispatch)
            .await
            .first()
            .cloned()
    })
    .await;
    checks.record(
        "the first run dispatches the ticket",
        dispatched.is_some(),
        format!(
            "{:?}",
            dispatched.as_ref().map(|event| event.occurred_at.clone())
        ),
    );

    // 硬杀编排器（宿主流失订阅者，但 attempt 与 claim 都还在宿主手里）。
    first.sigkill();
    checks.record(
        "the orchestrator process was SIGKILLed while its attempt was still running",
        !first.is_alive(),
        format!("pid {}", first.pid()),
    );
    let after_kill = events_of_kind(&client, TICKET, EventKind::Dispatch).await;
    checks.record(
        "the SIGKILL left exactly one dispatch on the event ledger",
        after_kill.len() == 1,
        format!("{} dispatch event(s)", after_kill.len()),
    );

    // 重启：同一个宿主、同一份事件账、同一张被 claim 的票。
    let mut second = OrchestratorRun::spawn(&env, scratch.path().join("orchestrator-2.log"));
    // 等足够多的 tick（重启后的对账重建到「看到活跃 attempt」之间也要几个 tick）。
    let ticked = poll(Duration::from_secs(30), || async {
        (second
            .log()
            .lines()
            .filter(|line| is_tick_line(line))
            .count()
            >= 5)
            .then_some(())
    })
    .await;
    checks.record(
        "the restarted orchestrator is really ticking (its own log says so)",
        ticked.is_some(),
        format!(
            "{} tick line(s), {} log bytes",
            second
                .log()
                .lines()
                .filter(|line| is_tick_line(line))
                .count(),
            second.log().len()
        ),
    );

    // 无双派：两个进程的生命期里，那张票仍然只有**一条** dispatch 事件。
    // 机制：重启后的第一个 tick 从宿主三源对账重建，看到「最后一条 dispatch 之后没有终态
    // 事件」⇒ 活跃 attempt ⇒ 不派发；claim 也仍然是它自己的（claim CAS 没有第二次机会可言）。
    let after_restart = events_of_kind(&client, TICKET, EventKind::Dispatch).await;
    checks.record(
        "no double dispatch: the ticket still has exactly one dispatch across both runs",
        after_restart.len() == 1,
        format!("{} dispatch event(s)", after_restart.len()),
    );
    let claims = events_of_kind(&client, TICKET, EventKind::ClaimAcquired).await;
    checks.record(
        "the restart re-claims idempotently at most (the claim CAS is not bypassed)",
        claims.len() <= 1,
        format!("{} claim_acquired event(s)", claims.len()),
    );
    checks.record(
        "the restarted orchestrator is still running",
        second.is_alive(),
        format!("pid {}", second.pid()),
    );

    assert!(checks.all_ok(), "端到端没跑绿：\n{}", checks.render());
    assert!(
        checks.len() >= 7,
        "防空洞：至少 7 条机械断言，实际 {}",
        checks.len()
    );
}

// ---------------------------------------------------------------------------
// 演练 3：非法 workflow.json → 继续用 LKG 编排 + 持续标注
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_corrupt_workflow_json_keeps_orchestrating_on_the_last_known_good() {
    if missing_binaries() {
        return;
    }
    let scratch = tempfile::tempdir().expect("scratch dir");
    let repo = scratch.path().join("repo");
    let worktrees = scratch.path().join("worktrees");
    // ① 先播一份**合法**的配置。
    scaffold_repo(&repo, &workflow_json());

    let host = TestHost::spawn(
        &[TICKET_NUMBER],
        &[
            (
                "FARM_AGENT_PROGRAM",
                fake_pi_bin().unwrap().to_string_lossy().into_owned(),
            ),
            ("FAKE_PI_SCENARIO", "silence".to_string()),
            (
                "FARM_MERGE_TEST_COMMAND",
                passing_test_script(scratch.path()),
            ),
        ],
    )
    .await;
    let client = host.client();

    // ② 触发一次 worktree 建立：宿主在**这一刻**从 `repo_path` 读 `.farm/workflow.json`
    //    （`farm-host/src/workspace.rs`：建立 worktree 后 `load_repo`），合法 ⇒ `fresh` + 存 LKG。
    bootstrap_mapping(&client, &repo, &worktrees).await;
    let fresh = client.workflow_config(SPEC).await.expect("admin face");
    let mut checks = Checks::default();
    checks.record(
        "the seeded workflow.json loads as the fresh effective config and becomes the LKG",
        fresh.effective.is_some()
            && fresh.source == Some(farm_sdk::workflow::EffectiveSource::Fresh),
        format!("source {:?}", fresh.source),
    );

    // ③ 把配置文件改成非法的，再触发一次建立：宿主回退 LKG（绝不 panic）。
    std::fs::write(repo.join(".farm/workflow.json"), "{ this is not json").expect("corrupt");
    let second = WorktreeEstablishRequest {
        spec_id: SPEC.to_string(),
        integration_branch: INTEGRATION.to_string(),
        ticket_id: "seed-2".to_string(),
        repo_path: repo.to_string_lossy().into_owned(),
        worktree_path: worktrees.join("seed-2").to_string_lossy().into_owned(),
        worktree_branch: "farm/seed-2".to_string(),
        start_point: Some(INTEGRATION.to_string()),
    };
    let established = client.establish_worktree(&second).await;
    checks.record(
        "the host keeps serving worktree establish with a corrupt workflow.json",
        established.is_ok(),
        format!("{established:?}"),
    );
    let lkg = client.workflow_config(SPEC).await.expect("admin face");
    checks.record(
        "the admin face reports last_known_good with an effective config (the host fell back)",
        lkg.effective.is_some()
            && lkg.source == Some(farm_sdk::workflow::EffectiveSource::LastKnownGood),
        format!("source {:?}, last_alert {:?}", lkg.source, lkg.last_alert),
    );

    // ④ 编排器照常编排（不是 idle）：票照样被派发。
    let env = OrchestratorEnv::new(host.addr, &repo.to_string_lossy(), &worktrees);
    let orchestrator = OrchestratorRun::spawn(&env, scratch.path().join("orchestrator.log"));
    let dispatched = poll(Duration::from_secs(90), || async {
        events_of_kind(&client, TICKET, EventKind::Dispatch)
            .await
            .first()
            .cloned()
    })
    .await;
    checks.record(
        "the orchestrator keeps orchestrating on the LKG config (the ticket still gets dispatched)",
        dispatched.is_some(),
        format!(
            "{:?}",
            dispatched.as_ref().map(|event| event.occurred_at.clone())
        ),
    );

    // ⑤ 持续标注：编排器的日志里有 LKG 告警（ADR-0004 的「标注」最小实现）。
    let annotated = poll(Duration::from_secs(30), || async {
        orchestrator
            .log()
            .contains("workflow_config_last_known_good")
            .then_some(())
    })
    .await;
    checks.record(
        "the orchestrator annotates that it is running on the last-known-good config",
        annotated.is_some(),
        "alert=workflow_config_last_known_good".to_string(),
    );
    let log = orchestrator.log();
    checks.record(
        "the annotation names the config it is running on (repo_id + when the LKG was stored)",
        log.contains(SPEC) && log.contains("last_known_good_at"),
        format!("log {} bytes", log.len()),
    );

    assert!(checks.all_ok(), "端到端没跑绿：\n{}", checks.render());
    assert!(
        checks.len() >= 6,
        "防空洞：至少 6 条机械断言，实际 {}",
        checks.len()
    );
}

// 占位的 env 名断言：票面闭集五的**名字**在集成测试里也被钉住（改名字会在这里红）。
#[test]
fn the_ticket_env_names_are_the_ones_the_ticket_lists() {
    assert_eq!(ENV_HOST, "FARM_HOST");
    assert_eq!(ENV_ASSIGNEE, "FARM_ASSIGNEE");
    assert_eq!(ENV_WORKTREE_PATH, "FARM_WORKTREE_PATH");
    assert_eq!(ENV_WORKTREE_BRANCH, "FARM_WORKTREE_BRANCH");
}

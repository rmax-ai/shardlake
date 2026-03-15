# Autonomous Development Loop

This document explains the repository's autonomous issue and pull request loop for human operators. It covers the principles the loop is built on, the concrete control flow in the shell driver, the GitHub label state machine, and the points where human intervention is still required.

## Purpose

The loop is a repository-level operator workflow that repeatedly:

1. reconciles the next issues that are ready for implementation
2. assigns actionable issue work to Copilot
3. triages draft and open pull requests into explicit review states
4. reviews a bounded number of pull requests
5. attempts a bounded number of merges
6. decides whether the next iteration should run immediately or sleep

The goal is not "full automation at any cost". The goal is deterministic, restartable progress with explicit safety boundaries. The loop should move straightforward work forward while refusing to invent certainty around ambiguous reviews, merge conflicts, unclear blocker state, or policy decisions.

## Main Components

The loop is split into three layers in serialized mode.

### 1. Shell driver

`loop_iteration.sh` is the outer control loop. It is responsible for:

- selecting the orchestrator prompt
- ensuring the operator checkout stays on `main` and clean while each iteration runs from a fresh dedicated worktree created from `origin/main`
- running one iteration through the Copilot CLI
- storing iteration logs under `tmp/loop_iterations/`
- extracting the final numbered report and control block from the iteration log into a JSON sidecar
- extracting the machine-readable control markers
- deciding whether to sleep before the next pass

The shell driver is the source of truth for runtime behavior such as iteration count, sleep timing, log naming, and failure handling.

The primary checkout is operational state, not an execution workspace. It must stay on `main`, remain clean, and must not be used for iteration execution or PR branch commands.

### 2. Orchestrator prompt

`.github/prompts/loop_iteration.prompt.md` defines the intended workflow for a single serialized iteration. It tells the agent what stages to run, in what order, how much work to do per stage, and what the final human-readable report must contain.

This prompt is the behavioral contract for the loop. It defines:

- the stage order
- the labels that act as workflow state
- deterministic ordering rules
- safety constraints around ambiguous items
- the requirement to report carry-forward state for the next pass

For concurrent local execution, use `.github/prompts/loop_reconcile.prompt.md` as the repository-wide reconciler prompt and the `worker-*.prompt.md` prompts for claimed per-item execution. The serialized orchestrator remains useful as a single-process mode and as the default entrypoint for `loop_iteration.sh`.

### 3. Final control block

The orchestrator prompt emits a small machine-readable block at the end of each completed iteration log:

```text
BEGIN_LOOP_CONTROL
PRS_PROCESSED: <number>
ALL_WAITING_ON_OTHER_AGENTS: <yes|no>
SLEEP_NEXT_ITERATION: <yes|no>
END_LOOP_CONTROL
```

The shell script parses that block, stores the final report as JSON, and decides whether the loop should sleep before continuing.

## Design Principles

The loop follows a small set of operating principles.

### Deterministic ordering

- Issues and pull requests are processed in ascending numeric order.
- The loop does not pick arbitrary items opportunistically.
- Bounded work per iteration prevents one noisy PR from starving the rest of the queue.

This matters because operators need to be able to explain why a specific item was or was not handled on a given pass.

### Labels are the workflow state machine

The loop does not rely on informal prose in issue or PR bodies to determine state. Instead, it uses GitHub labels as the primary workflow state.

Current labels defined by the orchestrator:

| Label | Meaning |
| ----- | ------- |
| `ready-to-implement` | The issue is in the bounded implementation queue |
| `ready-for-draft-check` | A draft PR appears ready for a readiness review |
| `ready-for-open-review` | An open non-draft PR is ready for review handling |
| `ready-to-merge` | An open PR has completed review handling and is ready for a final merge pass |
| `needs-human` | A PR is blocked on manual intervention and must not be advanced automatically |

This gives operators a visible state machine in GitHub instead of hidden in local process memory.

In concurrent local mode, labels remain the state machine but not the locking mechanism. Eligibility still lives on GitHub labels; worker ownership must be tracked separately through a lease or claim protocol so multiple loops do not race on the same item.

### Actor guard rail

- The loop only processes issues and pull requests whose author login passes a normalized actor guard rail: `copilot-swe-agent`, `copilot-swe-agent[bot]`, `app/copilot-swe-agent`, or `rmax`.
- GitHub App actor identities must be normalized before policy checks. In practice, `app/copilot-swe-agent` is treated as the GitHub App form of `copilot-swe-agent`, not as a separate ineligible actor.
- Author identity is treated as required eligibility metadata for triage, assignment, review, and merge stages.
- If author identity cannot be determined safely, the item is skipped as policy-blocked instead of being advanced optimistically.

### One goal per stage

Each stage prompt is meant to perform one narrow task. The orchestrator prompt explicitly forbids merging stage responsibilities. This keeps failure modes smaller and makes the logs easier to audit.

### Idempotence over cleverness

Stages are expected to reconcile state from fresh GitHub snapshots and then make only the changes required to reach the desired state. Re-running the loop should therefore converge on the same label and review state, not create duplicate noise.

### Conservative advancement

If eligibility is ambiguous, the item is not advanced. The loop is designed to skip uncertain transitions rather than risk moving a PR into the wrong state.

### Human-readable report plus machine-readable control

Every iteration produces a narrative report for operators and a separate machine-readable control block for the shell script. That split is deliberate:

- operators need context, reasoning, and carry-forward state
- the shell driver needs stable, parseable control markers

## Detailed Control Flow

This section describes the exact logic in `loop_iteration.sh` when run in serialized mode.

### Environment and prerequisites

The script requires these commands to exist:

- `copilot`
- `tee`
- `awk`
- `sleep`
- `python3`

It also sets stable CLI output defaults before running the loop:

```bash
export GH_PAGER=cat
export NO_COLOR=1
export CLICOLOR=0
```

Those settings reduce pager and ANSI-control-sequence noise in logs.

### Configurable environment variables

The driver reads these variables:

| Variable | Default | Purpose |
| -------- | ------- | ------- |
| `COPILOT_BIN` | `copilot` | Copilot CLI executable to run |
| `LOOP_PROMPT_PATH` | `.github/prompts/loop_iteration.prompt.md` | Prompt entrypoint executed inside the iteration worktree |
| `MAX_ITERATIONS` | `100` | Maximum number of loop iterations before the script exits |
| `WAIT_SECONDS` | `300` | Sleep duration between iterations when the control block says to wait |
| `GH_PAGER` | `cat` | Pager override exported into the loop environment |
| `NO_COLOR` | `1` | Disables color in subprocess output |
| `CLICOLOR` | `0` | Disables colorized CLI output |

### Per-iteration sequence

For each iteration, the shell driver does the following:

1. Creates timestamped log paths under `tmp/loop_iterations/`.
1. Fetches `origin/main` without changing the operator checkout and creates a fresh detached iteration worktree from that remote ref under `tmp/iteration_worktrees/`.
1. Runs the orchestrator prompt with:

   ```bash
   "$COPILOT_BIN" --model gpt-5.4 --allow-all-tools -p "follow instructions in ${LOOP_PROMPT_PATH}"
   ```

   from inside the fresh iteration worktree, with `SHARDLAKE_PRIMARY_ROOT` exported so PR-review stages can create their own dedicated worktrees under the primary repository root.

1. Streams that output to the main iteration log.
1. If the Copilot command fails, exits immediately with the same status.
1. Extracts the final numbered report and control block from the completed log into a JSON sidecar.
1. Extracts `PRS_PROCESSED`, `ALL_WAITING_ON_OTHER_AGENTS`, and `SLEEP_NEXT_ITERATION` from the final control block in the main log.
1. Normalizes boolean values so only `yes` and `no` are used operationally.
1. Applies one safety fallback: if `SLEEP_NEXT_ITERATION=no`, `PRS_PROCESSED=0`, and `ALL_WAITING_ON_OTHER_AGENTS=yes`, the shell overrides `SLEEP_NEXT_ITERATION` to `yes`.
1. Verifies the operator checkout is still on the original `main` HEAD and removes the temporary iteration worktree when it is clean.
1. Sleeps for `WAIT_SECONDS` before the next pass when the current iteration is not the last allowed iteration and the final sleep decision is `yes`.

The loop does not stop early just because no work was found. It is a polling loop that either runs immediately again or sleeps and tries later, until `MAX_ITERATIONS` is reached or a command fails.

## Concurrent Local Loop Model

The repository now has a prompt split for concurrent local execution without GitHub Actions.

### Reconciler prompt

`.github/prompts/loop_reconcile.prompt.md` performs repository-wide reconciliation only. It:

- triages the `ready-to-implement` issue queue
- assigns currently ready issues
- reconciles draft-PR labels
- reconciles open-PR labels
- publishes the draft-review, open-review, and merge queues
- emits scheduler guidance for an external local scheduler

The reconciler must not claim a PR or issue, must not check out PR branches, and must not merge.

### Worker prompts

The concurrent worker prompts are:

- `.github/prompts/worker-review-draft-pr.prompt.md`
- `.github/prompts/worker-review-open-pr.prompt.md`
- `.github/prompts/worker-merge-pr.prompt.md`

The checked-in worker launcher is `loop_worker.sh`. It resolves the next eligible PR for a single lane with `gh`, acquires a lease, revalidates the claimed PR's current state and head SHA with `gh`, runs the matching worker prompt with explicit inputs, and then releases the lease.

The checked-in scheduler launcher is `loop_scheduler.sh`. It runs one reconcile pass, dispatches `draft-review` and `open-review` workers concurrently when claimable work exists, then runs the `merge` worker as a single lane. It repeats for a bounded number of cycles and respects the reconciler's `SLEEP_NEXT_ITERATION` control signal between cycles.

Each worker prompt assumes a target item has already been claimed. It must:

- operate on exactly one claimed PR
- verify the lease owner before any destructive or durable write
- revalidate label, state, and head SHA after claim
- use a dedicated PR worktree
- stop cleanly if the lease is missing, expired, or no longer owned by that worker

The checked-in lease helper is `tools/loop_claim.sh`. It uses one remote ref per claimed PR lane at `refs/heads/loop-claims/<lane>/pr-<number>`. The tip commit of that ref contains a `lease.json` payload with owner id, lane, target PR number, expected head SHA, and UTC acquisition and expiry timestamps.

Protocol rules:

- `acquire` creates a new lease or steals an expired lease by pushing a new synthetic commit to the lane ref
- `renew` extends an active lease owned by the same worker by pushing a child commit to the same ref
- `release` deletes the ref with compare-and-swap semantics using the currently observed ref tip
- `inspect` reads the remote ref and reports whether the lease is `active`, `expired`, or `missing`
- acquire and renew are compare-and-swap updates because they only succeed when the observed ref tip is still current at push time

### Control blocks

Serialized mode still uses `.github/prompts/loop_control.prompt.md` and its `BEGIN_LOOP_CONTROL` block.

Concurrent reconciler mode uses `.github/prompts/loop_reconcile_control.prompt.md` and a different machine-readable block:

```text
BEGIN_RECONCILE_CONTROL
CLAIMABLE_WORK_EXISTS: <yes|no>
ALL_WAITING_ON_OTHER_AGENTS: <yes|no>
SLEEP_NEXT_ITERATION: <yes|no>
END_RECONCILE_CONTROL
```

The concurrent loop now has a thin checked-in shell scheduler in `loop_scheduler.sh`. The serialized driver can still be pointed at the reconciler prompt via `LOOP_PROMPT_PATH` for supervised reconcile-only runs while keeping the serialized prompt as the default.

## Worktree Isolation Model

The loop now uses two separate layers of transient worktrees:

1. an iteration worktree created fresh from `origin/main` for every orchestrator run
2. per-PR worktrees created under `tmp/pr_worktrees/` for draft review, open review, and merge stages

This separation means the operator checkout is never the execution context for either the main loop or PR branch operations.

## Stage Logic

The orchestrator prompt defines a single-iteration workflow with bounded action per stage.

### 1. Issue triage

The issue triage stage maintains the `ready-to-implement` queue.

Its core rules are:

- only child issues of open epics are eligible
- only child issues whose author login passes the actor guard rail are eligible
- blocked issues are not eligible
- at most 5 open issues may hold `ready-to-implement`
- the queue is filled deterministically by epic number, then child issue number

Operationally, this stage turns a larger backlog into a bounded implementation frontier.

### 2. Issue assignment

The assignment stage hands actionable work to Copilot. The current checked-in supporting prompt for this responsibility is `.github/prompts/assign-open-non-blocked-epic-issues.prompt.md`, which operates on open, non-blocked epic child issues and assigns them to `copilot-swe-agent`.

Operators should treat assignment as a separate concern from triage:

- triage decides which issues belong in the active queue
- assignment decides which actionable issues should be handed to the agent
- assignment skips issues whose author login fails the actor guard rail

### 3. Draft PR triage

The orchestrator includes a draft PR triage stage before any detailed draft review. The purpose is to decide which draft PRs should be marked `ready-for-draft-check` and which should remain waiting.

At a high level this stage should:

- inspect open draft PRs from a repository snapshot
- skip PRs whose author login fails the actor guard rail
- determine whether agent work appears complete enough for readiness review, using GitHub issue events as the primary completion signal
- reconcile the `ready-for-draft-check` label
- record skipped items and why they remain waiting

In practice, the loop should derive draft readiness from the ordered PR issue events emitted via the `copilot-swe-agent` GitHub App. A draft PR is eligible for `ready-for-draft-check` only when the latest relevant Copilot work event is `copilot_work_finished`. If the latest relevant event is `copilot_work_started`, or if no relevant Copilot work events are visible yet, the PR must stay out of `ready-for-draft-check` for that iteration.

To keep that rule deterministic across prompts and workers, the repository includes `python3 tools/copilot_pr_state.py --repo <owner>/<repo> --pr <number>`. The loop should use that helper for every draft-PR readiness decision instead of inferring readiness from weaker signals such as missing pending banners, Copilot-authored commits, review requests, or a new draft matching the same pattern as earlier completed drafts.

### 4. Open PR triage

Open PR triage follows the same normalized actor restriction as the other stages: only open non-draft PRs authored by `copilot-swe-agent`, `copilot-swe-agent[bot]`, `app/copilot-swe-agent`, or `rmax` are eligible to receive loop-managed review or merge labels.

The orchestrator then triages open non-draft PRs for active review. The goal is to identify which open PRs should carry `ready-for-open-review` and which are still waiting for review input, CI, or manual decisions.

### 5. Draft PR review

The loop reviews at most one draft PR per iteration: the lowest-numbered open draft PR labeled `ready-for-draft-check`.

This stage is intended to answer one question: can this PR leave draft safely?

Its review criteria include:

- branch is safe to check out
- PR scope matches its summary and linked issue
- repository quality gates pass
- docs coverage is present for user-visible changes
- obvious implementation gaps are absent

The checked-in prompt for this responsibility in serialized mode is `.github/prompts/review-ready-draft-pr.prompt.md`.

### 6. Open PR review

The loop reviews at most one open non-draft PR per iteration: the lowest-numbered PR labeled `ready-for-open-review`.

This stage is intended to answer a different question: what is the minimum remaining work before merge?

Its review criteria include:

- current review threads and PR comments
- status checks
- repository quality gates
- docs completeness
- must-fix versus safe-to-defer follow-up work

The checked-in prompt for this responsibility in serialized mode is `.github/prompts/review-ready-open-pr.prompt.md`.

### 7. Merge pass

The loop attempts at most one merge candidate per iteration: the lowest-numbered open non-draft PR labeled `ready-to-merge`.

The merge pass must not advance a PR while blocking checks, unresolved blocking feedback, merge conflicts, or policy blockers remain.

If any stage detects merge conflicts on a PR, the loop should add the `needs-human` label so the manual handoff is explicit in GitHub state.

## Dedicated Worktree Rule

Any serialized review or merge prompt, and all concurrent worker prompts, require a dedicated git worktree rather than the repository's main checkout.

This rule exists for four reasons:

1. it reduces the chance of trampling the operator's main checkout
2. it isolates per-PR validation runs
3. it makes cleanup explicit when switching between multiple PRs in one loop cycle
4. it preserves the invariant that the repository's primary checkout stays on `main` and only pulls from the remote default branch

Human operators should expect review stages to fail or stop early if the local checkout is dirty enough to make branch switching unsafe.

## Carry-Forward State

Each iteration report must explicitly summarize work that still exists after the current pass. The orchestrator requires four carry-forward buckets:

- draft PRs still waiting on agent completion
- open PRs waiting on Copilot or Codex review comments
- PRs needing reviewer discussion or manual decisions
- merge candidates blocked by checks, conflicts, or policy

This is important because the next operator, or the next loop pass, needs a concise handoff rather than just a list of what succeeded.

## Loop Control Semantics

The final report contains human-readable loop control, and the shell relies on the machine-readable block synthesized by `.github/prompts/loop_control.prompt.md` from inside the main loop iteration session.

The meanings are:

| Marker | Meaning |
| ------ | ------- |
| `PRS_PROCESSED` | Count of PRs actually handled by the detailed PR-review or merge stages |
| `ALL_WAITING_ON_OTHER_AGENTS` | `yes` only when no PRs were processed and all skipped draft PRs were skipped solely because agent work was still pending or ambiguous |
| `SLEEP_NEXT_ITERATION` | `yes` only when the loop should back off before the next pass |

The control prompt is deliberately conservative. If a log is incomplete or ambiguous, it must emit:

- `PRS_PROCESSED: 0`
- `ALL_WAITING_ON_OTHER_AGENTS: no`

That prevents the shell from assuming the loop is safely idle when it is not.

To avoid spending an extra premium request on a second top-level Copilot run, the main loop iteration prompt delegates this synthesis to the loop-control prompt as a subagent and appends the returned block to the same iteration log.

## How To Run The Loop

### Basic usage

Run the loop from the repository root:

```bash
./loop_iteration.sh
```

### Reconciler-only usage

For a single repository-wide reconciliation pass without PR execution stages:

```bash
LOOP_PROMPT_PATH=.github/prompts/loop_reconcile.prompt.md MAX_ITERATIONS=1 ./loop_iteration.sh
```

### Concurrent scheduler usage

For the full concurrent local loop with shell-level worker dispatch:

```bash
./loop_scheduler.sh
```

For one bounded scheduler cycle:

```bash
./loop_scheduler.sh --once
```

To disable selected lanes during supervised operation:

```bash
./loop_scheduler.sh --skip-draft-review --skip-merge
```

### Single-pass usage

For one explicit operator-driven pass without a polling wait cycle:

```bash
MAX_ITERATIONS=1 ./loop_iteration.sh
```

### Faster polling during active supervision

```bash
WAIT_SECONDS=60 MAX_ITERATIONS=20 ./loop_iteration.sh
```

### Using a non-default Copilot CLI binary

```bash
COPILOT_BIN=/path/to/copilot MAX_ITERATIONS=1 ./loop_iteration.sh
```

## What Operators Should Verify Before Running

Before starting the loop, operators should verify:

- `gh` authentication is valid for this repository
- the Copilot CLI is installed and can run with tool access
- the repository checkout is on `main`
- the main checkout is clean, because the loop will refuse to run otherwise
- required prompt files in `.github/prompts/` are present and in sync with the orchestrator's stage list

The last point matters because the checked-in orchestrator currently describes some stage-specific prompt filenames that do not exactly match every prompt filename present in `.github/prompts/`.

At the time of writing:

- `issue-triage.prompt.md` exists and matches the issue-triage responsibility
- `assign-ready-issues.prompt.md` exists for serialized and concurrent assignment behavior
- `triage-draft-prs.prompt.md` exists for draft-PR queue reconciliation
- `triage-open-prs.prompt.md` exists for open-PR queue reconciliation
- `review-ready-draft-pr.prompt.md`, `review-ready-open-pr.prompt.md`, and `merge-ready-pr.prompt.md` remain the serialized single-process execution prompts
- `loop_reconcile.prompt.md`, `loop_reconcile_control.prompt.md`, and the `worker-*.prompt.md` files define the concurrent local prompt split
- `tools/loop_claim.sh` now implements the lease protocol expected by the worker prompts
- `loop_worker.sh` now implements a lane-aware worker launcher that resolves queue entries, acquires leases, revalidates claimed PRs, and invokes the matching worker prompt with explicit inputs
- `loop_scheduler.sh` now implements the thin local scheduler that polls via reconcile cycles and dispatches the per-lane workers

Operators should treat prompt-name drift as an operational risk. Keep the orchestrator and the prompt directory synchronized before relying on unattended loop execution.

The PR-review prompts also assume `tools/prepare_pr_worktree.sh` is present. If that helper is missing or cannot prepare a dedicated worktree, the PR stage should fail rather than fall back to the active checkout.

## Logs and Auditability

Each iteration writes timestamped artifacts under `tmp/loop_iterations/`:

- `iteration_<n>_<timestamp>.log`
- `iteration_<n>_<timestamp>.json`

The main log contains:

- the full orchestrator output
- the final loop-control block

The JSON sidecar contains:

- the full final numbered report as text
- structured sections for each report heading
- parsed carry-forward and loop-control fields
- parsed machine-readable control values

These logs are the primary audit trail for:

- what the loop attempted
- why an item was skipped
- which PRs were processed
- why the next iteration did or did not sleep

## Failure Behavior

The shell driver exits immediately when:

- the Copilot iteration command exits non-zero
- JSON sidecar extraction fails
- required control markers are missing
- `PRS_PROCESSED` is not numeric
- a required local command is missing

This is intentional. Silent continuation after malformed control data would make the polling logic unsafe.

## When Humans Must Intervene

The loop is not meant to replace all operator judgment. Human intervention is still required when:

- a PR has merge conflicts
- when merge conflicts are detected, the loop should label the PR `needs-human`
- a PR needs product or architecture decisions rather than mechanical review
- GitHub permissions, token scopes, or branch protections block automation
- prompt files drift out of sync with the orchestrator contract
- issue dependency state is ambiguous or missing
- the working tree is dirty enough that safe checkout is not possible

## Recommended Operator Practice

For active supervision, the safest pattern is:

1. run a single pass with `MAX_ITERATIONS=1`
2. inspect the generated log and carry-forward state
3. fix any prompt drift, permission issue, or merge-conflict blocker
4. only then move to repeated polling

For unattended runs, use a bounded `MAX_ITERATIONS`, keep `WAIT_SECONDS` explicit, and review the latest log files regularly. The loop is robust when state is clear, but it is intentionally conservative when the repository needs human judgment.

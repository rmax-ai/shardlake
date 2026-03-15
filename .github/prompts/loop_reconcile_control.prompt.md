---
name: loop-reconcile-control
description: Derive the machine-readable scheduler control block from a completed concurrent reconciler log.
---
Primary goal: read the completed reconcile log passed in the invoking prompt and emit only the machine-readable reconcile control block.

This prompt is responsible only for concurrent reconciler control synthesis. It must not inspect repository state, call GitHub, or propose workflow changes.

Inputs:

- The invoking prompt will include the path to a completed reconciler log.

Definitions:

- `Claimable work exists` means at least one issue or PR is currently eligible for a worker lane after reconciliation.
- `All waiting on other agents` means no claimable work exists and every skipped draft PR was skipped only because agent work was still pending or ambiguous. Any open-review PR, ready-to-merge PR, human-blocked item, or policy-blocked item means the answer is `no`.

Instructions:

1. Read the referenced reconciliation log.
2. Use the report content in that log to determine:
   - `CLAIMABLE_WORK_EXISTS`
   - `ALL_WAITING_ON_OTHER_AGENTS`
   - `SLEEP_NEXT_ITERATION`
3. Set `SLEEP_NEXT_ITERATION` to `yes` if and only if `CLAIMABLE_WORK_EXISTS` is `no` and `ALL_WAITING_ON_OTHER_AGENTS` is `yes`; otherwise set it to `no`.
4. If the log is incomplete or ambiguous, be conservative:
   - set `CLAIMABLE_WORK_EXISTS` to `no`
   - set `ALL_WAITING_ON_OTHER_AGENTS` to `no`
5. Emit only the control block below, with no prose before or after it.

Required output:

BEGIN_RECONCILE_CONTROL
CLAIMABLE_WORK_EXISTS: <yes|no>
ALL_WAITING_ON_OTHER_AGENTS: <yes|no>
SLEEP_NEXT_ITERATION: <yes|no>
END_RECONCILE_CONTROL
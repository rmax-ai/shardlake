---
name: loop-control
description: Derive the machine-readable loop control block from a completed loop iteration log.
---
Primary goal: read the completed loop iteration log passed in the invoking prompt and emit only the machine-readable loop control block.

This prompt is responsible only for loop control synthesis. It must not inspect repository state, call GitHub, or propose workflow changes.
It is intended to run as a final subagent step inside the main loop iteration session so the orchestrator does not need a second top-level Copilot invocation.

Inputs:

- The invoking prompt will include the path to a completed loop iteration log.

Definitions:

- `PRs processed` means the number of PRs actually handled by:
  - `review-ready-draft-pr.prompt.md`
  - `review-ready-open-pr.prompt.md`
  - `merge-ready-pr.prompt.md`
- `All waiting on other agents` means no PRs were processed and every skipped draft PR was skipped only because agent work was still pending or ambiguous. Any ready-for-open-review PR, ready-to-merge PR, human-blocked item, or policy-blocked item means the answer is `no`.

Instructions:

1. Read the referenced loop iteration log.
2. Use the report content in that log to determine:
   - `PRS_PROCESSED`
   - `ALL_WAITING_ON_OTHER_AGENTS`
   - `SLEEP_NEXT_ITERATION`
3. Set `SLEEP_NEXT_ITERATION` to `yes` if and only if `PRS_PROCESSED` is `0` and `ALL_WAITING_ON_OTHER_AGENTS` is `yes`; otherwise set it to `no`.
4. If the log is incomplete or ambiguous, be conservative:
   - set `PRS_PROCESSED` to `0`
   - set `ALL_WAITING_ON_OTHER_AGENTS` to `no`
5. Emit only the control block below, with no prose before or after it.

Required output:

BEGIN_LOOP_CONTROL
PRS_PROCESSED: <number>
ALL_WAITING_ON_OTHER_AGENTS: <yes|no>
SLEEP_NEXT_ITERATION: <yes|no>
END_LOOP_CONTROL

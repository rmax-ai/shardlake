---
name: assign-open-non-blocked-epic-issues
description: Use this prompt to find actionable child issues under a GitHub epic and assign the open, currently non-blocked ones to copilot-swe-agent.
---
Given a GitHub epic in this repository, identify all currently actionable child issues and assign them to `copilot-swe-agent`.

Input:
- Epic reference: may be either:
  - an issue number in this repository, or
  - a full GitHub issue URL

Requirements:
1. Resolve the input as a GitHub issue in this repository.
   - Accept either a bare issue number or a full issue URL.
   - If the input is invalid or points to a different repository, stop and report the problem clearly.
2. Retrieve all sub-issues of the epic.
3. For each sub-issue, retrieve:
   - issue number
   - title
   - state (`open` or `closed`)
   - its `blocked_by` dependency list
4. Determine current actionability using issue state, not just dependency presence.
   - A sub-issue is currently blocked only if it has at least one blocker whose state is still `open`.
   - Closed blockers do not count as active blockers.
5. Identify the actionable set:
   - sub-issue state is `open`
   - the sub-issue has zero open blockers
6. Assign every actionable sub-issue to `copilot-swe-agent`.
   - Emit `assign_to_agent` for each actionable issue with `issue_number=<n>` and `agent="copilot-swe-agent"`.
   - Do not use direct repository writes to perform assignment.
7. Do not assign:
   - closed issues
   - issues that still have open blockers
8. Be idempotent.
   - If an actionable issue is already assigned to `copilot-swe-agent`, do not fail.
   - Continue processing the remaining issues.
9. After assignment, verify the final state.

Execution guidance:
- Use `gh` CLI.
- Use the GitHub sub-issues API to retrieve child issues if needed.
- Use the GitHub issue dependency API to retrieve `blocked_by` relationships.
- When evaluating blockers, inspect the blocker issues’ current states.
- Prefer a workflow that first computes the actionable set, then performs assignment.
- Perform assignment by emitting the approved `assign_to_agent` safe output, not by editing issues directly.
- If the epic has no sub-issues, report that clearly and do not attempt assignment.
- If there are no actionable issues, report that clearly and do not treat it as an error.
- If automation is blocked on a needed human decision for a specific issue, ensure the `needs-human` label exists, add it to that issue, and leave a concise evidence-based issue comment describing the decision needed, why the prompt could not proceed safely, and the minimum next action.

Recommended workflow:
1. Resolve the epic reference.
2. Fetch all sub-issues.
3. For each sub-issue, fetch its blockers.
4. Filter blockers to only those still open.
5. Build a review table containing:
   - issue number
   - title
   - issue state
   - open blockers
   - actionable (`yes` or `no`)
6. Assign each actionable issue to `copilot-swe-agent`.
7. Verify assignments and summarize results.

Output format:
1. Resolved epic
2. Sub-issues reviewed
3. Actionable open non-blocked issues
4. Issues assigned to `copilot-swe-agent`
5. Notes or errors

Preferred output template:

- Epic: `#<number>` <title>
- Sub-issues reviewed:
  - `#<n>` <title> — state: `<open|closed>` — open blockers: `[ ... ]` — actionable: `<yes|no>`
- Actionable open non-blocked issues:
  - `#<n>` <title>
- Assigned to `copilot-swe-agent`:
  - `#<n>`
- Notes:
  - <any skipped items, invalid input handling, already-assigned items, or empty results>

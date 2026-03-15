---
name: check-draft-pr
description: Use this prompt to inspect a draft PR by URL or number, check it out locally, run shardlake's Rust quality gates, and report whether it is ready to move out of draft.
---
Primary goal: decide whether this draft PR is ready to leave draft and enter normal review.

Given a GitHub pull request URL or PR number for this repository, review the draft PR and perform the standard pre-review checks.

Input:
- A PR URL like `https://github.com/rmax-ai/shardlake/pull/123`, or
- A PR number like `123`

Requirements:
1. Resolve the PR from the provided URL or number.
2. Before checking it out, verify the local working tree is safe to switch:
   - inspect git status
   - if there are local modifications that could be overwritten or create confusion, stop and report that checkout was not attempted
3. Fetch and inspect the PR metadata:
   - title
   - draft state
   - base branch
   - head branch
   - body/summary
   - linked issues if present
   - changed files
4. Check out the PR locally with `gh pr checkout`.
5. Review the scope of the diff against the PR summary and any linked issue acceptance criteria.
6. Run the repository quality gates from the checked-out PR branch:
   - `cargo fmt --check`
   - `cargo clippy -- -D warnings`
   - `cargo test`
   - `cargo doc --no-deps`
7. Verify docs coverage for user-visible changes:
   - if the PR changes CLI behavior, config, API behavior, data formats, manifests, storage layout, or other user-facing behavior, confirm the relevant files in `docs/` were updated
8. Check basic PR hygiene:
   - confirm linked issue references are present when expected
   - confirm labels or milestone context are not obviously misleading when they are in use
   - confirm the PR body includes enough summary, testing, and docs context to review the change
9. Identify whether the PR appears ready to move from draft to review, based on:
   - passing checks
   - scope matching the PR description
   - acceptance criteria coverage
   - docs completeness
   - obvious missing tests or follow-up work
10. Update PR metadata if needed:
   - if the current PR title is misleading, incomplete, or no longer matches the actual scope, update it
   - if the PR body is missing important summary, acceptance-criteria mapping, linked issue context, docs notes, or follow-up callouts, update it
   - keep edits factual, concise, and aligned with the actual diff
11. Avoid duplicate reviewer noise:
   - before adding a PR comment, check whether the same conclusion is already captured in the PR body or an existing recent comment
   - before suggesting follow-up issues, check whether an equivalent open issue already exists
12. Add a PR comment if needed:
   - if the checks reveal blocking issues, unclear acceptance criteria, notable follow-up items, or readiness concerns, leave a concise PR comment summarizing them
   - prefer updating the PR body for durable factual context and using a comment for reviewer-facing conclusions or readiness concerns
   - if no extra comment would add value, do not add one
13. Every concern you report must be evidence-based:
   - tie each blocking item or readiness concern to a failing check, missing acceptance criterion, unresolved feedback, or direct code/doc/test observation
14. Do not make code changes unless explicitly asked. This prompt is for checkout, validation, and reporting.
15. If any command fails, continue gathering as much context as possible and report the failure clearly.

If any check in this prompt shows the PR has merge conflicts, ensure the `has-merge-conflicts` and `needs-human` labels exist, add both labels to the PR, and leave a concise evidence-based PR comment describing the conflict and the required human resolution.

If automation is blocked on a needed human decision, policy call, or other manual judgment, ensure the `needs-human` label exists, add it to the PR, and leave a concise evidence-based PR comment describing the decision needed, why the prompt could not proceed safely, and the minimum next action.

Execution guidance:
- Use `gh pr view <input> --json number,title,isDraft,state,baseRefName,headRefName,author,body,labels,milestone,closingIssuesReferences,files,statusCheckRollup,reviews,comments` to resolve the PR and gather metadata when structured output is useful.
- Use `gh pr checkout <input>` to switch to the PR branch.
- Use `gh pr edit <input> --title <title> --body-file <file>` when the title or body should be corrected.
- Use `gh pr comment <input> --body-file <file>` when a reviewer-facing summary comment would be useful.
- Use `gh issue list --state open --search <query>` before proposing or creating a follow-up issue to avoid duplicates.
- Prefer concise, evidence-based reporting.
- When judging acceptance criteria, use linked issues or issue references from the PR body when available.
- If no linked issue exists, evaluate only against the PR description and changed files.
- Only update the title/body when the improvement is clearly justified by the diff or review findings.
- Only add a PR comment when it creates useful reviewer context beyond the final local report.
- Prefer a PR body update over a PR comment when recording durable factual context such as scope, testing notes, or docs notes.
- Be explicit about whether the branch was actually checked out.
- Leave the repository on the checked-out PR branch unless the user asks otherwise.
- Use a simple decision checklist before the final recommendation: checks pass, scope matches, docs are adequate, tests are adequate, and no obvious implementation gaps remain.
- Use `gh pr view <input> --json mergeable` or another `gh` read that exposes the same state when you need to determine whether the PR is merge-conflicted.

Output format:
1. PR summary
   - PR number and title
   - draft status
   - base and head branches
   - whether checkout succeeded
2. Changed scope
   - key files or modules touched
   - whether the diff matches the stated goal
3. Check results
   - one line each for `fmt`, `clippy`, `test`, and `doc`
4. Docs review
   - whether docs updates were needed
   - whether they are present
5. PR hygiene review
   - linked issue/reference status
   - label or milestone notes if relevant
   - whether the body has sufficient summary/testing/docs context
6. Acceptance criteria review
   - covered / unclear / missing
7. Limitations
   - anything that could not be checked
   - any command failures or missing context affecting confidence
8. PR metadata updates
   - whether title or description changed
   - short summary of what changed and why
9. PR comment
   - whether a comment was added
   - short summary of what it said and why
10. Recommendation
   - `ready for review`, `needs fixes before review`, or `could not fully assess`
11. Decision checklist
   - checks pass: yes/no
   - scope matches stated goal: yes/no
   - docs adequate: yes/no
   - tests adequate: yes/no
   - obvious implementation gaps remain: yes/no
12. Next actions
   - short actionable bullets

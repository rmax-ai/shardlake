---
name: check-draft-pr
description: Use this prompt to inspect a draft PR by URL or number, check it out locally, run shardlake's Rust quality gates, and report whether it is ready to move out of draft.
---
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
8. Identify whether the PR appears ready to move from draft to review, based on:
   - passing checks
   - scope matching the PR description
   - acceptance criteria coverage
   - docs completeness
   - obvious missing tests or follow-up work
9. Update PR metadata if needed:
   - if the current PR title is misleading, incomplete, or no longer matches the actual scope, update it
   - if the PR body is missing important summary, acceptance-criteria mapping, linked issue context, docs notes, or follow-up callouts, update it
   - keep edits factual, concise, and aligned with the actual diff
10. Add a PR comment if needed:
   - if the checks reveal blocking issues, unclear acceptance criteria, notable follow-up items, or readiness concerns, leave a concise PR comment summarizing them
   - if no extra comment would add value, do not add one
11. Do not make code changes unless explicitly asked. This prompt is for checkout, validation, and reporting.
12. If any command fails, continue gathering as much context as possible and report the failure clearly.

Execution guidance:
- Use `gh pr view <input>` to resolve the PR.
- Use `gh pr checkout <input>` to switch to the PR branch.
- Use `gh pr edit <input>` (or equivalent GitHub API calls) if the title/body should be corrected.
- Use `gh pr comment <input>` if a reviewer-facing summary comment would be useful.
- Prefer concise, evidence-based reporting.
- When judging acceptance criteria, use linked issues or issue references from the PR body when available.
- If no linked issue exists, evaluate only against the PR description and changed files.
- Only update the title/body when the improvement is clearly justified by the diff or review findings.
- Only add a PR comment when it creates useful reviewer context beyond the final local report.
- Be explicit about whether the branch was actually checked out.
- Leave the repository on the checked-out PR branch unless the user asks otherwise.

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
5. Acceptance criteria review
   - covered / unclear / missing
6. PR metadata updates
   - whether title or description changed
   - short summary of what changed and why
7. PR comment
   - whether a comment was added
   - short summary of what it said and why
8. Recommendation
   - `ready for review`, `needs fixes before review`, or `could not fully assess`
9. Next actions
   - short actionable bullets

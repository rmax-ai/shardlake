---
name: review-open-pr
description: Use this prompt to inspect an open PR by URL or number, check it out locally, review feedback and quality gates, and report what must change before merge.
---
Given a GitHub pull request URL or PR number for this repository, review the open PR and perform a merge-readiness assessment.

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
   - open/closed/merged state
   - draft state
   - base branch
   - head branch
   - author
   - body/summary
   - linked issues if present
   - labels if present
   - changed files
   - CI / status checks
4. Fetch the discussion context:
   - PR reviews and their states
   - review comments / review threads
   - general PR comments
   - identify unresolved or still-relevant feedback when possible
5. Check out the PR locally with `gh pr checkout`.
6. Review the scope of the diff against the PR summary and any linked issue acceptance criteria.
7. Run the repository quality gates from the checked-out PR branch:
   - `cargo fmt --check`
   - `cargo clippy -- -D warnings`
   - `cargo test`
   - `cargo doc --no-deps`
8. Verify docs coverage for user-visible changes:
   - if the PR changes CLI behavior, config, API behavior, data formats, manifests, storage layout, or other user-facing behavior, confirm the relevant files in `docs/` were updated
9. Review feedback quality and disposition:
   - summarize the main review themes
   - separate comments that appear blocking from comments that look like polish, naming, ergonomics, or future enhancements
   - note any feedback that is already addressed by the current diff or appears stale
10. Recommend follow-up handling for non-blocking items:
   - identify items that should be fixed before merge
   - identify items that can reasonably be deferred to follow-up GitHub issues
   - for each deferrable item, provide extended issue-ready detail:
     - suggested issue title
     - problem statement
     - why it is safe to defer
     - acceptance criteria / concrete next steps
11. Apply your own high-signal PR review checks in addition to the above:
   - watch for missing tests around changed behavior
   - watch for missing docs or migration notes
   - watch for risky API or schema changes without explicit versioning
   - watch for mismatch between PR description and actual diff scope
   - watch for dead code, TODO-shaped gaps, or partially implemented behavior
   - watch for error handling regressions, panics, unchecked assumptions, or obvious maintainability concerns
12. Update PR metadata if needed:
   - if the PR title no longer reflects the actual scope, update it
   - if the PR body is missing important summary, issue linkage, acceptance-criteria coverage, docs notes, or follow-up context, update it
   - keep edits factual, concise, and directly supported by the diff and review findings
13. Add a PR comment if needed:
   - if maintainers or reviewers would benefit from a concise summary of must-fix items, deferred issues, or merge-readiness concerns, leave a PR comment
   - if the PR already has sufficient context and no added comment is needed, do not add one
14. Do not make code changes unless explicitly asked. This prompt is for checkout, validation, and reporting.
15. If any command fails, continue gathering as much context as possible and report the failure clearly.

Execution guidance:
- Use `gh pr view <input>` to resolve the PR.
- Use `gh pr checkout <input>` to switch to the PR branch.
- Use `gh pr edit <input>` (or equivalent GitHub API calls) if the title/body should be corrected.
- Use `gh pr comment <input>` if a reviewer-facing summary comment would add value.
- Prefer concise, evidence-based reporting.
- When judging acceptance criteria, use linked issues or issue references from the PR body when available.
- If no linked issue exists, evaluate only against the PR description, review discussion, and changed files.
- Treat explicit reviewer requests, failed CI, broken quality gates, and missing acceptance-criteria coverage as strong signals for must-fix items.
- Treat stylistic cleanup, naming preferences, optional refactors, and broader future work as candidates for follow-up issues unless they hide correctness or maintainability risks.
- Only update the title/body when the change clearly improves reviewer understanding or accuracy.
- Only add a PR comment when it usefully summarizes findings, decisions, or deferred work beyond the final local report.
- Be explicit about whether the branch was actually checked out.
- Leave the repository on the checked-out PR branch unless the user asks otherwise.

Output format:
1. PR summary
   - PR number and title
   - author
   - state and draft status
   - base and head branches
   - whether checkout succeeded
2. Changed scope
   - key files or modules touched
   - whether the diff matches the stated goal
3. Review feedback summary
   - main review themes
   - unresolved / likely active feedback
   - stale or already-addressed feedback if any
4. Check results
   - one line each for `fmt`, `clippy`, `test`, and `doc`
   - status check summary
5. Docs review
   - whether docs updates were needed
   - whether they are present
6. Acceptance criteria review
   - covered / unclear / missing
7. Must-fix before merge
   - concise bullets with reasons
8. Safe to defer as follow-up issues
   - for each item, include:
     - suggested issue title
     - why defer is acceptable
     - problem statement
     - acceptance criteria / next steps
9. PR metadata updates
   - whether title or description changed
   - short summary of what changed and why
10. PR comment
   - whether a comment was added
   - short summary of what it said and why
11. Recommendation
   - `ready to merge`, `needs author updates`, `needs reviewer discussion`, or `could not fully assess`
12. Next actions
   - short actionable bullets

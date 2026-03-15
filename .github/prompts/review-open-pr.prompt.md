---
name: review-open-pr
description: Use this prompt to inspect an open PR by URL or number, check it out locally, review feedback and quality gates, and report what must change before merge.
---
Primary goal: decide whether this open PR is ready to merge, or identify the minimum author/reviewer follow-up needed first.

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
   - use `gh pr view --json ...` for supported pull request fields such as `reviews`, `comments`, `latestReviews`, `files`, `commits`, `statusCheckRollup`, and `reviewDecision`
   - do not request `reviewThreads` via `gh pr view --json`; that field is not supported by the GitHub CLI JSON view output
   - when thread-level state is needed, use `gh api graphql` to query `pullRequest { reviewThreads(...) { nodes { isResolved isOutdated comments(...) { nodes { author { login } body path outdated originalPosition diffHunk createdAt } } } } }`
5. Check out the PR locally with `gh pr checkout`.
6. Review the scope of the diff against the PR summary and any linked issue acceptance criteria.
7. Run the repository quality gates from the checked-out PR branch:
   - `cargo fmt --check`
   - `cargo clippy -- -D warnings`
   - `cargo test`
   - `cargo doc --no-deps`
8. Verify docs coverage for user-visible changes:
   - if the PR changes CLI behavior, config, API behavior, data formats, manifests, storage layout, or other user-facing behavior, confirm the relevant files in `docs/` were updated
9. Check basic PR hygiene:
   - confirm linked issue references are present when expected
   - confirm labels or milestone context are not obviously misleading when they are in use
   - confirm the PR body includes enough summary, testing, docs, and follow-up context to support merge review
10. Review feedback quality and disposition:
   - summarize the main review themes
   - separate comments that appear blocking from comments that look like polish, naming, ergonomics, or future enhancements
   - note any feedback that is already addressed by the current diff or appears stale
11. Recommend follow-up handling for non-blocking items:
   - identify items that should be fixed before merge
   - identify items that can reasonably be deferred to follow-up GitHub issues
   - for each deferrable item, provide extended issue-ready detail:
     - suggested issue title
     - problem statement
     - why it is safe to defer
     - acceptance criteria / concrete next steps
12. Avoid duplicate reviewer or issue noise:
   - before suggesting or creating a follow-up issue, check whether an equivalent open issue already exists
   - before adding a PR comment, check whether the same conclusion is already captured in the PR body or an existing recent comment
13. Apply your own high-signal PR review checks in addition to the above:
   - watch for missing tests around changed behavior
   - watch for missing docs or migration notes
   - watch for risky API or schema changes without explicit versioning
   - watch for mismatch between PR description and actual diff scope
   - watch for dead code, TODO-shaped gaps, or partially implemented behavior
   - watch for error handling regressions, panics, unchecked assumptions, or obvious maintainability concerns
14. Update PR metadata if needed:
   - if the PR title no longer reflects the actual scope, update it
   - if the PR body is missing important summary, issue linkage, acceptance-criteria coverage, docs notes, or follow-up context, update it
   - keep edits factual, concise, and directly supported by the diff and review findings
15. Add a PR comment if needed:
   - if maintainers or reviewers would benefit from a concise summary of must-fix items, deferred issues, or merge-readiness concerns, leave a PR comment
   - prefer updating the PR body for durable factual context and using a comment for reviewer-facing conclusions, must-fix summaries, or deferred-work mapping
   - if the PR already has sufficient context and no added comment is needed, do not add one
16. Every must-fix or deferrable item must be evidence-based:
   - tie it to a failing check, unresolved review thread, missing acceptance criterion, or direct code/doc/test observation
17. Do not make code changes unless explicitly asked. This prompt is for checkout, validation, and reporting.
18. If any command fails, continue gathering as much context as possible and report the failure clearly.

If any check in this prompt shows the PR has merge conflicts, ensure the `has-merge-conflicts` label exists and add it to the PR. Do not add `needs-human` for plain conflict detection in this prompt. Add `needs-human` only if a prior conflict-resolution attempt for the current head/base pair is already documented as failed or another independent required human design, architecture, policy, or product decision blocks safe automation. Leave a concise evidence-based PR comment describing whether the PR is being routed to the conflict-resolution lane or escalated to `needs-human`, and report the conflict clearly as the blocker.

If automation is blocked on a needed human decision, policy call, or other manual judgment, ensure the `needs-human` label exists, add it to the PR, and leave a concise evidence-based PR comment describing the decision needed, why the prompt could not proceed safely, and the minimum next action.

Execution guidance:
- Use `gh pr view <input> --json number,title,isDraft,state,baseRefName,headRefName,author,body,labels,milestone,closingIssuesReferences,files,statusCheckRollup,reviews,comments,reviewDecision` to resolve the PR and gather metadata when structured output is useful.
- Use `gh api graphql -f query='query($owner:String!,$repo:String!,$number:Int!){ repository(owner:$owner,name:$repo){ pullRequest(number:$number){ reviewThreads(first:50){ nodes { isResolved isOutdated comments(first:20){ nodes { author { login } body path outdated originalPosition diffHunk createdAt } } } } } } }' -F owner=<owner> -F repo=<repo> -F number=<pr-number>` when you need review thread resolution state; do not ask `gh pr view --json` for `reviewThreads`.
- Use `gh pr checkout <input>` to switch to the PR branch.
- Use `gh pr edit <input> --title <title> --body-file <file>` when the title or body should be corrected.
- Use `gh pr comment <input> --body-file <file>` when a reviewer-facing summary comment would add value.
- Use `gh issue list --state open --search <query>` before proposing or creating a follow-up issue to avoid duplicates.
- Prefer concise, evidence-based reporting.
- When judging acceptance criteria, use linked issues or issue references from the PR body when available.
- If no linked issue exists, evaluate only against the PR description, review discussion, and changed files.
- Treat explicit reviewer requests, failed CI, broken quality gates, and missing acceptance-criteria coverage as strong signals for must-fix items.
- Treat stylistic cleanup, naming preferences, optional refactors, and broader future work as candidates for follow-up issues unless they hide correctness or maintainability risks.
- Only update the title/body when the change clearly improves reviewer understanding or accuracy.
- Only add a PR comment when it usefully summarizes findings, decisions, or deferred work beyond the final local report.
- Prefer a PR body update over a PR comment when recording durable factual context such as scope, testing notes, docs notes, or follow-up links.
- Be explicit about whether the branch was actually checked out.
- Leave the repository on the checked-out PR branch unless the user asks otherwise.
- Use a final decision checklist before the recommendation: checks pass, blocking feedback resolved, must-fix list empty, docs adequate, tests adequate, and deferred work is captured if needed.
- Use `gh pr view <input> --json mergeable` or another `gh` read that exposes the same state when you need to determine whether the PR is merge-conflicted.

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
6. PR hygiene review
   - linked issue/reference status
   - label or milestone notes if relevant
   - whether the body has sufficient summary/testing/docs/follow-up context
7. Acceptance criteria review
   - covered / unclear / missing
8. Must-fix before merge
   - concise bullets with reasons
9. Safe to defer as follow-up issues
   - for each item, include:
     - suggested issue title
     - why defer is acceptable
     - problem statement
     - acceptance criteria / next steps
10. Limitations
   - anything that could not be checked
   - any command failures or missing context affecting confidence
11. PR metadata updates
   - whether title or description changed
   - short summary of what changed and why
12. PR comment
   - whether a comment was added
   - short summary of what it said and why
13. Recommendation
   - `ready to merge`, `needs author updates`, `needs reviewer discussion`, or `could not fully assess`
14. Decision checklist
   - checks pass: yes/no
   - blocking feedback resolved: yes/no
   - must-fix list empty: yes/no
   - docs adequate: yes/no
   - tests adequate: yes/no
   - deferred work captured if needed: yes/no
15. Next actions
   - short actionable bullets

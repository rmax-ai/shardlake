---
name: break-down-epic
description: Use this prompt to decompose a GitHub epic into a structured set of child issues with explicit dependencies, following shardlake's architectural boundaries and atomic file organization principles.
---
Given a GitHub epic issue in this repository, break it down into actionable child issues and wire the relationships in GitHub.

Requirements:
1. Read the epic issue fully, including goal, detailed tasks, acceptance criteria, and any existing child issues or dependency metadata.
1a. Preserve the epic's section numbering and meaning exactly. If the epic lists `2.2` as routing logic and `2.3` as metadata format, the child issue titles and summaries must keep that same mapping.
2. Infer a clean execution plan that:
   - keeps each child issue independently deliverable
   - minimizes merge conflicts
   - respects architectural boundaries already present in the codebase
   - makes dependencies explicit
3. Create child issues for the epic if they do not already exist.
4. Use GitHub issue relationships to:
   - add each child issue as a sub-issue of the epic
   - add blocked-by dependencies between child issues wherever sequencing matters
5. Update the epic body with:
   - a “Child issue breakdown” section
   - one checkbox per child issue
   - a short dependency summary
   - a compact dependency graph
6. Avoid duplicates:
   - list existing open issues first
   - reuse existing matching issues instead of creating near-duplicates
7. Keep issue titles concrete and scoped.
8. Each child issue body must include:
   - Parent issue reference
   - Summary
   - Dependency plan
   - Scope
   - Acceptance criteria
   - Docs update reminder if user-visible behavior may change
9. Prefer a dependency graph that is as shallow as possible while still correct.
10. After making changes, verify:
   - all intended sub-issue relationships exist
   - all intended blocked-by relationships exist
   - the epic body reflects the final issue numbers and dependency structure
   - each child issue title still matches the corresponding epic section number and task meaning
   - verification uses the actual returned issue numbers/ids from GitHub rather than assumed sequential numbers

Execution guidance:
- Use `gh` CLI.
- For sub-issues, use the GitHub issue relationship API if needed.
- For blocked-by links, use the GitHub issue dependency API if needed.
- If a direct `gh issue` command does not support the relationship feature, use `gh api`.
- When adding sub-issues through the API, use the child issue’s internal REST `id`, not its issue number.
- When adding blocked-by dependencies, pass the blocking issue’s internal REST `id`.
- Be idempotent: if a relationship already exists, do not fail; continue.

Output format:
1. Short summary of the plan
2. List of created or reused child issues with numbers
3. Dependency mapping in plain English
4. Confirmation that GitHub sub-issue and dependency relationships were applied

If useful, here is a preferred child-issue template:

Title:
<epic section number> - <clear scoped task>

Body:
## Parent
- Parent issue: #<epic>

## Summary
<1-3 sentence summary>

## Dependency plan
- Depends on: <none or issue list>
- Blocks: <none or issue list>

## Scope
- ...
- ...
- ...

## Acceptance criteria
- ...
- ...
- ...

## Docs
- Update the relevant files in docs in the same PR for any user-visible schema, CLI, API, configuration, or storage-layout change.

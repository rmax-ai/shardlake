#!/usr/bin/env bash

set -euo pipefail

usage() {
  echo "usage: $0 <pr-number> [base-branch]" >&2
  exit 64
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
fi

PR_NUMBER="$1"
BASE_BRANCH="${2:-main}"
PRIMARY_REMOTE="${PRIMARY_REMOTE:-origin}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKTREE_ROOT="${PR_WORKTREE_DIR:-$REPO_ROOT/tmp/pr_worktrees}"
WORKTREE_PATH="$WORKTREE_ROOT/pr-$PR_NUMBER"
BASE_REF="$BASE_BRANCH"

if ! [[ "$PR_NUMBER" =~ ^[0-9]+$ ]]; then
  echo "invalid PR number: $PR_NUMBER" >&2
  exit 64
fi

mkdir -p "$WORKTREE_ROOT"

git -C "$REPO_ROOT" fetch "$PRIMARY_REMOTE" "$BASE_BRANCH" >/dev/null 2>&1 || true
if git -C "$REPO_ROOT" rev-parse --verify "$PRIMARY_REMOTE/$BASE_BRANCH" >/dev/null 2>&1; then
  BASE_REF="$PRIMARY_REMOTE/$BASE_BRANCH"
fi

if [[ -e "$WORKTREE_PATH" ]]; then
  git -C "$REPO_ROOT" worktree remove --force "$WORKTREE_PATH" >/dev/null 2>&1 || true
  rm -rf "$WORKTREE_PATH"
fi

git -C "$REPO_ROOT" worktree add --detach "$WORKTREE_PATH" "$BASE_REF" >/dev/null

printf '%s\n' "$WORKTREE_PATH"
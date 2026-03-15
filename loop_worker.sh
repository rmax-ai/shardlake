#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR"
COPILOT_BIN="${COPILOT_BIN:-copilot}"
PRIMARY_REMOTE="${PRIMARY_REMOTE:-origin}"
PRIMARY_BRANCH="${PRIMARY_BRANCH:-main}"
WORKER_ITERATION_DIR="${WORKER_ITERATION_DIR:-$REPO_ROOT/tmp/worker_iterations}"
WORKER_LOG_DIR="${WORKER_LOG_DIR:-$REPO_ROOT/tmp/loop_workers}"
CLAIM_TTL_SECONDS="${LOOP_CLAIM_TTL_SECONDS:-1800}"
GH_PAGER_VALUE="${GH_PAGER:-cat}"
NO_COLOR_VALUE="${NO_COLOR:-1}"
CLICOLOR_VALUE="${CLICOLOR:-0}"
GITHUB_REPOSITORY="${GITHUB_REPOSITORY:-rmax-ai/shardlake}"

usage() {
  cat >&2 <<'EOF'
usage: loop_worker.sh --lane <draft-review|open-review|merge> [--pr <number>] [--owner <owner-id>] [--ttl-seconds <seconds>]

Resolves one queue item for the selected lane with `gh`, acquires a lease with
`tools/loop_claim.sh`, revalidates the target PR, runs the matching worker prompt,
and releases the lease before exiting.
EOF
  exit 64
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 127
  fi
}

die() {
  echo "$*" >&2
  exit 64
}

current_branch() {
  git rev-parse --abbrev-ref HEAD
}

current_head() {
  git rev-parse HEAD
}

ensure_clean_primary_checkout() {
  if [[ -n "$(git status --porcelain)" ]]; then
    echo "[loop_worker] primary checkout is dirty; refusing to continue" >&2
    exit 1
  fi
}

prepare_primary_checkout() {
  local branch

  branch="$(current_branch)"
  if [[ "$branch" != "$PRIMARY_BRANCH" ]]; then
    echo "[loop_worker] primary checkout must start on ${PRIMARY_BRANCH}; found ${branch}" >&2
    exit 1
  fi

  ensure_clean_primary_checkout
  git fetch "$PRIMARY_REMOTE" "$PRIMARY_BRANCH"
  if ! git rev-parse "${PRIMARY_REMOTE}/${PRIMARY_BRANCH}" >/dev/null 2>&1; then
    echo "[loop_worker] unable to resolve ${PRIMARY_REMOTE}/${PRIMARY_BRANCH} after fetch" >&2
    exit 1
  fi
}

create_iteration_worktree() {
  local lane="$1"
  local timestamp="$2"
  local worktree_path="$WORKER_ITERATION_DIR/${lane}_${timestamp}"
  local remote_head
  local worktree_head

  mkdir -p "$WORKER_ITERATION_DIR"

  if [[ -e "$worktree_path" ]]; then
    git worktree remove --force "$worktree_path" >/dev/null 2>&1 || true
    rm -rf "$worktree_path"
  fi

  git worktree add --detach "$worktree_path" "${PRIMARY_REMOTE}/${PRIMARY_BRANCH}" >/dev/null

  remote_head="$(git rev-parse "${PRIMARY_REMOTE}/${PRIMARY_BRANCH}")"
  worktree_head="$(git -C "$worktree_path" rev-parse HEAD)"
  if [[ "$worktree_head" != "$remote_head" ]]; then
    echo "[loop_worker] iteration worktree HEAD ${worktree_head} did not match ${PRIMARY_REMOTE}/${PRIMARY_BRANCH} ${remote_head}" >&2
    exit 1
  fi

  printf '%s\n' "$worktree_path"
}

cleanup_iteration_worktree() {
  local worktree_path="$1"

  if [[ -z "$worktree_path" || ! -d "$worktree_path" ]]; then
    return
  fi

  if [[ -n "$(git -C "$worktree_path" status --porcelain 2>/dev/null || true)" ]]; then
    echo "[loop_worker] preserving dirty iteration worktree at ${worktree_path}" >&2
    return
  fi

  git worktree remove --force "$worktree_path"
}

assert_primary_checkout_unchanged() {
  local expected_branch="$1"
  local expected_head="$2"
  local branch
  local head

  branch="$(current_branch)"
  head="$(current_head)"

  if [[ "$branch" != "$expected_branch" ]]; then
    echo "[loop_worker] primary checkout moved to ${branch}; expected ${expected_branch}" >&2
    exit 1
  fi

  if [[ "$head" != "$expected_head" ]]; then
    echo "[loop_worker] primary checkout HEAD changed from ${expected_head} to ${head}" >&2
    exit 1
  fi

  ensure_clean_primary_checkout
}

prompt_path_for_lane() {
  case "$1" in
    draft-review)
      printf '.github/prompts/worker-review-draft-pr.prompt.md\n'
      ;;
    open-review)
      printf '.github/prompts/worker-review-open-pr.prompt.md\n'
      ;;
    merge)
      printf '.github/prompts/worker-merge-pr.prompt.md\n'
      ;;
    *)
      die "unsupported lane: $1"
      ;;
  esac
}

resolve_candidates() {
  local lane="$1"
  local target_pr="${2:-}"
  local payload_file

  if [[ -n "$target_pr" ]]; then
    payload_file="$(mktemp)"
    gh pr view "$target_pr" --repo "$GITHUB_REPOSITORY" --json number,title,isDraft,labels,headRefOid,baseRefName,author,state,url >"$payload_file"
    python3 - "$lane" "$target_pr" "$payload_file" <<'PY'
import json
import sys

lane = sys.argv[1]
target_pr = int(sys.argv[2])
payload_file = sys.argv[3]
with open(payload_file, encoding="utf-8") as handle:
    pr = json.load(handle)
labels = {label["name"] for label in pr.get("labels", [])}

def eligible(item: dict) -> bool:
    if item.get("state") != "OPEN":
        return False
    if lane == "draft-review":
        return item.get("isDraft") and "ready-for-draft-check" in labels
    if lane == "open-review":
        return (not item.get("isDraft")) and "ready-for-open-review" in labels and "ready-to-merge" not in labels
    if lane == "merge":
        return (not item.get("isDraft")) and "ready-to-merge" in labels
    raise SystemExit(f"unsupported lane: {lane}")

if pr.get("number") != target_pr:
    raise SystemExit(f"target PR mismatch: expected {target_pr}, got {pr.get('number')}")

if eligible(pr):
    print(json.dumps(pr, sort_keys=True))
PY
    rm -f "$payload_file"
    return
  fi

  payload_file="$(mktemp)"
  gh pr list --repo "$GITHUB_REPOSITORY" --state open --limit 200 --json number,title,isDraft,labels,headRefOid,baseRefName,author,state,url >"$payload_file"
  python3 - "$lane" "$payload_file" <<'PY'
import json
import sys

lane = sys.argv[1]
payload_file = sys.argv[2]
with open(payload_file, encoding="utf-8") as handle:
    prs = json.load(handle)

def eligible(item: dict) -> bool:
    labels = {label["name"] for label in item.get("labels", [])}
    if item.get("state") != "OPEN":
        return False
    if lane == "draft-review":
        return item.get("isDraft") and "ready-for-draft-check" in labels
    if lane == "open-review":
        return (not item.get("isDraft")) and "ready-for-open-review" in labels and "ready-to-merge" not in labels
    if lane == "merge":
        return (not item.get("isDraft")) and "ready-to-merge" in labels
    raise SystemExit(f"unsupported lane: {lane}")

for pr in sorted(prs, key=lambda item: item["number"]):
    if eligible(pr):
        print(json.dumps(pr, sort_keys=True))
PY
  rm -f "$payload_file"
}

json_field() {
  local payload="$1"
  local field_name="$2"

  python3 - "$payload" "$field_name" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
field_name = sys.argv[2]
value = payload
for part in field_name.split('.'):
    value = value.get(part) if isinstance(value, dict) else None
    if value is None:
        break
if value is None:
    print("")
else:
    print(value)
PY
}

validate_claimed_pr() {
  local lane="$1"
  local expected_head_sha="$2"
  local payload="$3"

  python3 - "$lane" "$expected_head_sha" "$payload" <<'PY'
import json
import sys

lane = sys.argv[1]
expected_head_sha = sys.argv[2]
payload = json.loads(sys.argv[3])
labels = {label["name"] for label in payload.get("labels", [])}
author = payload.get("author") or {}
author_login = author.get("login")
allowed_logins = {"copilot-swe-agent", "copilot-swe-agent[bot]", "app/copilot-swe-agent", "rmax"}
errors = []

if payload.get("state") != "OPEN":
    errors.append("PR is no longer open")

if payload.get("headRefOid") != expected_head_sha:
    errors.append(f"PR head SHA changed to {payload.get('headRefOid')}")

if author_login not in allowed_logins:
    errors.append(f"author login {author_login!r} fails the workflow actor guard rail")

if lane == "draft-review":
    if not payload.get("isDraft"):
        errors.append("PR is no longer draft")
    if "ready-for-draft-check" not in labels:
        errors.append("PR no longer has ready-for-draft-check")
elif lane == "open-review":
    if payload.get("isDraft"):
        errors.append("PR reverted to draft")
    if "ready-for-open-review" not in labels:
        errors.append("PR no longer has ready-for-open-review")
    if "ready-to-merge" in labels:
        errors.append("PR is already labeled ready-to-merge")
elif lane == "merge":
    if payload.get("isDraft"):
        errors.append("PR reverted to draft")
    if "ready-to-merge" not in labels:
        errors.append("PR no longer has ready-to-merge")
else:
    errors.append(f"unsupported lane: {lane}")

if errors:
    raise SystemExit("; ".join(errors))
PY
}

validate_draft_completion_state() {
  local pr_number="$1"
  local state_json

  state_json="$(python3 "$REPO_ROOT/tools/copilot_pr_state.py" --repo "$GITHUB_REPOSITORY" --pr "$pr_number")"
  python3 - "$state_json" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
if payload.get("ready_for_draft_check"):
    raise SystemExit(0)

reason = payload.get("reason") or payload.get("state") or "unknown draft readiness state"
raise SystemExit(f"draft PR lacks a current copilot_work_finished signal: {reason}")
PY
}

run_worker_prompt() {
  local prompt_path="$1"
  local iteration_worktree="$2"
  local output_file="$3"
  local prompt_text
  local command_status=0

  prompt_text="follow instructions in ${prompt_path}

Use only the gh CLI for GitHub reads and writes in this run. Do not use GitHub MCP tools, repository GitHub tools, or alternate GitHub access paths. If a required gh command fails, stop immediately and report the exact failure.

Worker inputs:
- target PR number: ${CLAIMED_PR_NUMBER}
- lease owner id: ${WORKER_OWNER_ID}
- lease ref name: ${LEASE_REF_NAME}
- expected PR head SHA: ${EXPECTED_HEAD_SHA}"

  set +e
  (
    cd "$iteration_worktree"
    export SHARDLAKE_PRIMARY_ROOT="$REPO_ROOT"
    export SHARDLAKE_ITERATION_WORKTREE="$iteration_worktree"
    export SHARDLAKE_TARGET_PR_NUMBER="$CLAIMED_PR_NUMBER"
    export SHARDLAKE_LEASE_OWNER_ID="$WORKER_OWNER_ID"
    export SHARDLAKE_LEASE_REF_NAME="$LEASE_REF_NAME"
    export SHARDLAKE_EXPECTED_HEAD_SHA="$EXPECTED_HEAD_SHA"
    export SHARDLAKE_WORKER_LANE="$LANE"
    "$COPILOT_BIN" --model gpt-5.4 --allow-all-tools --allow-url=github.com --add-dir /tmp --add-dir "$REPO_ROOT" -p "$prompt_text"
  ) | tee "$output_file"
  command_status=${PIPESTATUS[0]}
  set -e

  return "$command_status"
}

release_claim() {
  if [[ "$LEASE_ACQUIRED" != "yes" ]]; then
    return 0
  fi

  tools/loop_claim.sh release --owner "$WORKER_OWNER_ID" --ref "$LEASE_REF_NAME"
}

cleanup() {
  local exit_status=$?
  trap - EXIT
  set +e

  if [[ "$LEASE_ACQUIRED" == "yes" ]]; then
    if ! release_claim >/tmp/loop_worker_release.$$ 2>&1; then
      cat /tmp/loop_worker_release.$$ >&2
      if [[ $exit_status -eq 0 ]]; then
        exit_status=1
      fi
    fi
    rm -f /tmp/loop_worker_release.$$
  fi

  cleanup_iteration_worktree "$ITERATION_WORKTREE"

  if [[ -n "$PRIMARY_BRANCH_BEFORE" && -n "$PRIMARY_HEAD_BEFORE" ]]; then
    if ! assert_primary_checkout_unchanged "$PRIMARY_BRANCH_BEFORE" "$PRIMARY_HEAD_BEFORE"; then
      if [[ $exit_status -eq 0 ]]; then
        exit_status=1
      fi
    fi
  fi

  exit "$exit_status"
}

LANE=""
TARGET_PR=""
WORKER_OWNER_ID="${WORKER_OWNER_ID:-}"
CLAIM_TTL_OVERRIDE=""
ITERATION_WORKTREE=""
LEASE_REF_NAME=""
CLAIMED_PR_NUMBER=""
EXPECTED_HEAD_SHA=""
LEASE_ACQUIRED="no"
PRIMARY_BRANCH_BEFORE=""
PRIMARY_HEAD_BEFORE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lane)
      LANE="$2"
      shift 2
      ;;
    --pr|--pr-number)
      TARGET_PR="$2"
      shift 2
      ;;
    --owner)
      WORKER_OWNER_ID="$2"
      shift 2
      ;;
    --ttl-seconds)
      CLAIM_TTL_OVERRIDE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[[ -n "$LANE" ]] || usage
case "$LANE" in
  draft-review|open-review|merge)
    ;;
  *)
    die "unsupported lane: $LANE"
    ;;
esac

if [[ -n "$TARGET_PR" ]] && ! [[ "$TARGET_PR" =~ ^[0-9]+$ ]]; then
  die "PR number must be numeric"
fi

if [[ -n "$CLAIM_TTL_OVERRIDE" ]]; then
  CLAIM_TTL_SECONDS="$CLAIM_TTL_OVERRIDE"
fi

if ! [[ "$CLAIM_TTL_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  die "ttl-seconds must be a positive integer"
fi

if [[ -z "$WORKER_OWNER_ID" ]]; then
  WORKER_OWNER_ID="$(id -un 2>/dev/null || echo worker)-${LANE}-$$-$(date -u +%Y%m%dT%H%M%SZ)"
fi

require_command "$COPILOT_BIN"
require_command git
require_command gh
require_command python3
require_command tee

PROMPT_PATH="$(prompt_path_for_lane "$LANE")"
if [[ ! -f "$REPO_ROOT/$PROMPT_PATH" ]]; then
  die "prompt file not found: $PROMPT_PATH"
fi

mkdir -p "$WORKER_LOG_DIR"
cd "$REPO_ROOT"

export GH_PAGER="$GH_PAGER_VALUE"
export NO_COLOR="$NO_COLOR_VALUE"
export CLICOLOR="$CLICOLOR_VALUE"

gh auth status >/dev/null
gh repo view "$GITHUB_REPOSITORY" --json nameWithOwner >/dev/null

prepare_primary_checkout
PRIMARY_BRANCH_BEFORE="$(current_branch)"
PRIMARY_HEAD_BEFORE="$(current_head)"
trap cleanup EXIT

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
ITERATION_WORKTREE="$(create_iteration_worktree "$LANE" "$timestamp")"

echo "[loop_worker] lane: $LANE"
echo "[loop_worker] prompt: $PROMPT_PATH"
echo "[loop_worker] worker owner id: $WORKER_OWNER_ID"
echo "[loop_worker] iteration worktree: $ITERATION_WORKTREE"

candidate_found="no"
while IFS= read -r candidate_json; do
  [[ -n "$candidate_json" ]] || continue
  candidate_found="yes"
  pr_number="$(json_field "$candidate_json" number)"
  head_sha="$(json_field "$candidate_json" headRefOid)"
  pr_url="$(json_field "$candidate_json" url)"

  echo "[loop_worker] considering PR #${pr_number} (${pr_url})"

  claim_output="$(tools/loop_claim.sh acquire --lane "$LANE" --pr "$pr_number" --owner "$WORKER_OWNER_ID" --head-sha "$head_sha" --ttl-seconds "$CLAIM_TTL_SECONDS")" || claim_status=$?
  claim_status="${claim_status:-0}"
  if [[ "$claim_status" -ne 0 ]]; then
    claim_state="$(json_field "$claim_output" status 2>/dev/null || true)"
    if [[ "$claim_state" == "held" || "$claim_state" == "conflict" ]]; then
      echo "[loop_worker] claim skipped for PR #${pr_number}: ${claim_state}"
      unset claim_status
      continue
    fi
    echo "$claim_output" >&2
    exit "$claim_status"
  fi
  unset claim_status

  LEASE_ACQUIRED="yes"
  LEASE_REF_NAME="$(json_field "$claim_output" lease_ref)"
  CLAIMED_PR_NUMBER="$pr_number"
  EXPECTED_HEAD_SHA="$head_sha"
  echo "$claim_output"

  fresh_pr_json="$(gh pr view "$CLAIMED_PR_NUMBER" --repo "$GITHUB_REPOSITORY" --json number,title,isDraft,labels,headRefOid,baseRefName,author,state,url)"
  if ! validate_claimed_pr "$LANE" "$EXPECTED_HEAD_SHA" "$fresh_pr_json"; then
    echo "[loop_worker] claimed PR #${CLAIMED_PR_NUMBER} no longer matches lane requirements; releasing claim" >&2
    exit 0
  fi
  if [[ "$LANE" == "draft-review" ]] && ! validate_draft_completion_state "$CLAIMED_PR_NUMBER"; then
    echo "[loop_worker] claimed PR #${CLAIMED_PR_NUMBER} is labeled ready-for-draft-check without a current copilot_work_finished event; releasing claim" >&2
    exit 0
  fi

  log_file="$WORKER_LOG_DIR/${LANE}_pr${CLAIMED_PR_NUMBER}_${timestamp}.log"
  echo "[loop_worker] log: $log_file"
  if run_worker_prompt "$PROMPT_PATH" "$ITERATION_WORKTREE" "$log_file"; then
    echo "[loop_worker] worker completed for PR #${CLAIMED_PR_NUMBER}"
    exit 0
  fi

  worker_status=$?
  echo "[loop_worker] worker prompt failed for PR #${CLAIMED_PR_NUMBER} with status ${worker_status}" >&2
  exit "$worker_status"
done < <(resolve_candidates "$LANE" "$TARGET_PR")

if [[ "$candidate_found" == "no" ]]; then
  if [[ -n "$TARGET_PR" ]]; then
    echo "[loop_worker] target PR #${TARGET_PR} is not currently eligible for lane ${LANE}" >&2
  else
    echo "[loop_worker] no eligible PR found for lane ${LANE}"
  fi
fi

exit 0

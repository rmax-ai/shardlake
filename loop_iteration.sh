#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR"
PROMPT_PATH="${LOOP_PROMPT_PATH:-.github/prompts/loop_iteration.prompt.md}"
COPILOT_BIN="${COPILOT_BIN:-copilot}"
MAX_ITERATIONS="${MAX_ITERATIONS:-100}"
WAIT_SECONDS="${WAIT_SECONDS:-300}"
LOG_DIR="$REPO_ROOT/tmp/loop_iterations"
ITERATION_WORKTREE_DIR="${ITERATION_WORKTREE_DIR:-$REPO_ROOT/tmp/iteration_worktrees}"
PRIMARY_REMOTE="${PRIMARY_REMOTE:-origin}"
PRIMARY_BRANCH="${PRIMARY_BRANCH:-main}"
GH_PAGER_VALUE="${GH_PAGER:-cat}"
NO_COLOR_VALUE="${NO_COLOR:-1}"
CLICOLOR_VALUE="${CLICOLOR:-0}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 127
  fi
}

extract_marker() {
  local marker="$1"
  local file="$2"

  LC_ALL=C LANG=C awk -v key="$marker" '
    function trim(value) {
      sub(/^[[:space:]]+/, "", value)
      sub(/[[:space:]]+$/, "", value)
      return value
    }

    BEGIN {
      in_block = 0
      block_seen = 0
      value = ""
      legacy_value = ""
    }

    {
      line = trim($0)
      gsub(/\r/, "", line)
      gsub(/^[[:space:]]*[-*]?[[:space:]]*/, "", line)
      gsub(/^[`*]+/, "", line)
      gsub(/[`*]+$/, "", line)

      if (line == "BEGIN_LOOP_CONTROL" || line == "BEGIN_RECONCILE_CONTROL") {
        in_block = 1
        block_seen = 1
        next
      }

      if (line == "END_LOOP_CONTROL" || line == "END_RECONCILE_CONTROL") {
        in_block = 0
        next
      }

      if (index(line, key ":") == 1) {
        parsed_value = trim(substr(line, length(key) + 2))
        gsub(/^[`*]+/, "", parsed_value)
        gsub(/[`*]+$/, "", parsed_value)

        if (in_block) {
          value = parsed_value
        } else if (legacy_value == "") {
          legacy_value = parsed_value
        }
      }
    }

    END {
      if (value != "") {
        print value
      } else if (!block_seen && legacy_value != "") {
        print legacy_value
      }
    }
  ' "$file"
}

detect_control_mode() {
  local file="$1"

  if grep -q '^BEGIN_RECONCILE_CONTROL$' "$file"; then
    printf 'reconcile\n'
  elif grep -q '^BEGIN_LOOP_CONTROL$' "$file"; then
    printf 'serialized\n'
  else
    printf 'unknown\n'
  fi
}

normalize_bool() {
  local value="${1:-}"
  value="$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')"
  if [[ "$value" == "yes" || "$value" == "true" ]]; then
    printf 'yes\n'
  else
    printf 'no\n'
  fi
}

run_prompt() {
  local prompt_text="$1"
  local output_file="$2"
  local iteration_worktree="$3"
  local command_status=0
  local gh_only_instruction

  gh_only_instruction="Use only the gh CLI for GitHub reads and writes in this run. Do not use GitHub MCP tools, repository GitHub tools, or alternate GitHub access paths. If a required gh command fails, stop immediately and report the exact failure instead of falling back."

  set +e
  (
    cd "$iteration_worktree"
    export SHARDLAKE_PRIMARY_ROOT="$REPO_ROOT"
    export SHARDLAKE_ITERATION_WORKTREE="$iteration_worktree"
    "$COPILOT_BIN" --model gpt-5.4 --allow-all-tools --allow-url=github.com --allow-all-paths --add-dir /tmp -p "$prompt_text

${gh_only_instruction}"
  ) | tee "$output_file"
  command_status=${PIPESTATUS[0]}
  set -e

  return "$command_status"
}

current_branch() {
  git rev-parse --abbrev-ref HEAD
}

current_head() {
  git rev-parse HEAD
}

ensure_clean_primary_checkout() {
  if [[ -n "$(git status --porcelain)" ]]; then
    echo "[loop_iteration] primary checkout is dirty; refusing to continue" >&2
    exit 1
  fi
}

prepare_primary_checkout() {
  local branch

  branch="$(current_branch)"
  if [[ "$branch" != "$PRIMARY_BRANCH" ]]; then
    echo "[loop_iteration] primary checkout must start on ${PRIMARY_BRANCH}; found ${branch}" >&2
    exit 1
  fi

  ensure_clean_primary_checkout

  git fetch "$PRIMARY_REMOTE" "$PRIMARY_BRANCH"
  if ! git rev-parse "${PRIMARY_REMOTE}/${PRIMARY_BRANCH}" >/dev/null 2>&1; then
    echo "[loop_iteration] unable to resolve ${PRIMARY_REMOTE}/${PRIMARY_BRANCH} after fetch" >&2
    exit 1
  fi
}

create_iteration_worktree() {
  local iteration="$1"
  local timestamp="$2"
  local worktree_path="$ITERATION_WORKTREE_DIR/iteration_${iteration}_${timestamp}"
  local remote_head
  local worktree_head

  mkdir -p "$ITERATION_WORKTREE_DIR"

  if [[ -e "$worktree_path" ]]; then
    git worktree remove --force "$worktree_path" >/dev/null 2>&1 || true
    rm -rf "$worktree_path"
  fi

  git worktree add --detach "$worktree_path" "${PRIMARY_REMOTE}/${PRIMARY_BRANCH}" >/dev/null

  remote_head="$(git rev-parse "${PRIMARY_REMOTE}/${PRIMARY_BRANCH}")"
  worktree_head="$(git -C "$worktree_path" rev-parse HEAD)"
  if [[ "$worktree_head" != "$remote_head" ]]; then
    echo "[loop_iteration] iteration worktree HEAD ${worktree_head} did not match ${PRIMARY_REMOTE}/${PRIMARY_BRANCH} ${remote_head}" >&2
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
    echo "[loop_iteration] preserving dirty iteration worktree at ${worktree_path}" >&2
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
    echo "[loop_iteration] primary checkout moved to ${branch}; expected ${expected_branch}" >&2
    exit 1
  fi

  if [[ "$head" != "$expected_head" ]]; then
    echo "[loop_iteration] primary checkout HEAD changed from ${expected_head} to ${head}" >&2
    exit 1
  fi

  ensure_clean_primary_checkout
}

write_iteration_json() {
  local iteration="$1"
  local timestamp="$2"
  local log_file="$3"
  local json_file="$4"
  local control_mode="$5"

  python3 - "$iteration" "$timestamp" "$log_file" "$json_file" "$control_mode" <<'PY'
import json
import re
import sys
from pathlib import Path


def slugify(value: str) -> str:
  return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def parse_named_lines(body: str) -> dict[str, str]:
  parsed: dict[str, str] = {}
  for raw_line in body.splitlines():
    line = raw_line.strip()
    if not line or ":" not in line:
      continue
    name, value = line.split(":", 1)
    parsed[slugify(name)] = value.strip()
  return parsed


iteration = int(sys.argv[1])
timestamp = sys.argv[2]
log_path = Path(sys.argv[3])
json_path = Path(sys.argv[4])
control_mode = sys.argv[5]

log_text = log_path.read_bytes().decode("utf-8", errors="replace")

report_start_matches = list(
  re.finditer(r"(?m)^1\. Ready-to-implement triage summary\s*$", log_text)
)
report_text = log_text[report_start_matches[-1].start():].strip() if report_start_matches else ""

section_titles = {
  "Ready-to-implement triage summary": "ready_to_implement_triage_summary",
  "Copilot assignment summary": "copilot_assignment_summary",
  "Draft PR triage summary": "draft_pr_triage_summary",
  "Open PR triage summary": "open_pr_triage_summary",
  "Draft PR review summary": "draft_pr_review_summary",
  "Open PR review summary": "open_pr_review_summary",
  "Merge summary": "merge_summary",
  "Carry-forward state": "carry_forward_state",
  "Loop control": "loop_control",
  "Machine-readable control block": "machine_readable_control_block",
}

section_matches = list(
  re.finditer(r"(?m)^(?P<number>10|[1-9])\. (?P<title>[^\n]+)\s*$", report_text)
)

sections: dict[str, dict[str, str]] = {}
for index, match in enumerate(section_matches):
  title = match.group("title").strip()
  key = section_titles.get(title, slugify(title))
  body_start = match.end()
  body_end = section_matches[index + 1].start() if index + 1 < len(section_matches) else len(report_text)
  body = report_text[body_start:body_end].strip()
  sections[key] = {
    "title": title,
    "body": body,
  }

serialized_control_match = re.search(
  r"(?ms)^BEGIN_LOOP_CONTROL\s*\n"
  r"PRS_PROCESSED:\s*(?P<prs_processed>\d+)\s*\n"
  r"ALL_WAITING_ON_OTHER_AGENTS:\s*(?P<all_waiting_on_other_agents>[^\n]+)\n"
  r"SLEEP_NEXT_ITERATION:\s*(?P<sleep_next_iteration>[^\n]+)\n"
  r"END_LOOP_CONTROL\s*$",
  report_text,
)
reconcile_control_match = re.search(
  r"(?ms)^BEGIN_RECONCILE_CONTROL\s*\n"
  r"CLAIMABLE_WORK_EXISTS:\s*(?P<claimable_work_exists>[^\n]+)\n"
  r"ALL_WAITING_ON_OTHER_AGENTS:\s*(?P<all_waiting_on_other_agents>[^\n]+)\n"
  r"SLEEP_NEXT_ITERATION:\s*(?P<sleep_next_iteration>[^\n]+)\n"
  r"END_RECONCILE_CONTROL\s*$",
  report_text,
)

control = {"mode": control_mode}
if serialized_control_match:
  control.update(
    {
      "prs_processed": int(serialized_control_match.group("prs_processed")),
      "all_waiting_on_other_agents": serialized_control_match.group("all_waiting_on_other_agents").strip(),
      "sleep_next_iteration": serialized_control_match.group("sleep_next_iteration").strip(),
    }
  )
elif reconcile_control_match:
  control.update(
    {
      "claimable_work_exists": reconcile_control_match.group("claimable_work_exists").strip(),
      "all_waiting_on_other_agents": reconcile_control_match.group("all_waiting_on_other_agents").strip(),
      "sleep_next_iteration": reconcile_control_match.group("sleep_next_iteration").strip(),
    }
  )
else:
  if control_mode == "serialized":
    control.update(
      {
        "prs_processed": None,
        "all_waiting_on_other_agents": None,
        "sleep_next_iteration": None,
      }
    )
  elif control_mode == "reconcile":
    control.update(
      {
        "claimable_work_exists": None,
        "all_waiting_on_other_agents": None,
        "sleep_next_iteration": None,
      }
    )

carry_forward = {}
if "carry_forward_state" in sections:
  carry_forward = parse_named_lines(sections["carry_forward_state"]["body"])

loop_control_summary = {}
if "loop_control" in sections:
  loop_control_summary = parse_named_lines(sections["loop_control"]["body"])

payload = {
  "json_version": 1,
  "iteration": iteration,
  "timestamp_utc": timestamp,
  "log_file": str(log_path),
  "control_mode": control_mode,
  "report_found": bool(report_text),
  "report_text": report_text,
  "sections": sections,
  "carry_forward": carry_forward,
  "loop_control_summary": loop_control_summary,
  "control": control,
}

json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY
}

require_command "$COPILOT_BIN"
require_command git
require_command tee
require_command awk
require_command grep
require_command sleep
require_command python3

if [[ ! -f "$REPO_ROOT/$PROMPT_PATH" ]]; then
  echo "[loop_iteration] prompt file not found: $PROMPT_PATH" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"

cd "$REPO_ROOT"

export GH_PAGER="$GH_PAGER_VALUE"
export NO_COLOR="$NO_COLOR_VALUE"
export CLICOLOR="$CLICOLOR_VALUE"

for ((iteration = 1; iteration <= MAX_ITERATIONS; iteration++)); do
  prepare_primary_checkout
  primary_branch_before="$(current_branch)"
  primary_head_before="$(current_head)"
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  log_file="$LOG_DIR/iteration_${iteration}_${timestamp}.log"
  json_file="$LOG_DIR/iteration_${iteration}_${timestamp}.json"
  iteration_worktree="$(create_iteration_worktree "$iteration" "$timestamp")"

  echo "[loop_iteration] starting iteration ${iteration}/${MAX_ITERATIONS}"
  echo "[loop_iteration] log: $log_file"
  echo "[loop_iteration] prompt: $PROMPT_PATH"
  echo "[loop_iteration] iteration worktree: $iteration_worktree"

  if run_prompt "follow instructions in ${PROMPT_PATH}" "$log_file" "$iteration_worktree"; then
    command_status=0
  else
    command_status=$?
  fi

  if [[ $command_status -ne 0 ]]; then
    cleanup_iteration_worktree "$iteration_worktree"
    echo "[loop_iteration] copilot command failed with status $command_status" >&2
    exit "$command_status"
  fi

  control_mode="$(detect_control_mode "$log_file")"

  if [[ "$control_mode" == "serialized" ]]; then
    prs_processed_raw="$(extract_marker "PRS_PROCESSED" "$log_file")"
    waiting_raw="$(extract_marker "ALL_WAITING_ON_OTHER_AGENTS" "$log_file")"
    sleep_next_raw="$(extract_marker "SLEEP_NEXT_ITERATION" "$log_file")"

    if [[ -z "$prs_processed_raw" || -z "$waiting_raw" || -z "$sleep_next_raw" ]]; then
      echo "[loop_iteration] missing serialized control markers in $log_file" >&2
      echo "[loop_iteration] expected BEGIN_LOOP_CONTROL/END_LOOP_CONTROL with PRS_PROCESSED, ALL_WAITING_ON_OTHER_AGENTS, and SLEEP_NEXT_ITERATION" >&2
      exit 1
    fi

    if ! [[ "$prs_processed_raw" =~ ^[0-9]+$ ]]; then
      echo "[loop_iteration] invalid PRS_PROCESSED value: $prs_processed_raw" >&2
      exit 1
    fi

    prs_processed="$prs_processed_raw"
    waiting="$(normalize_bool "$waiting_raw")"
    sleep_next="$(normalize_bool "$sleep_next_raw")"

    if [[ "$sleep_next" == "no" && "$prs_processed" == "0" && "$waiting" == "yes" ]]; then
      sleep_next="yes"
    fi
  elif [[ "$control_mode" == "reconcile" ]]; then
    claimable_work_exists_raw="$(extract_marker "CLAIMABLE_WORK_EXISTS" "$log_file")"
    waiting_raw="$(extract_marker "ALL_WAITING_ON_OTHER_AGENTS" "$log_file")"
    sleep_next_raw="$(extract_marker "SLEEP_NEXT_ITERATION" "$log_file")"

    if [[ -z "$claimable_work_exists_raw" || -z "$waiting_raw" || -z "$sleep_next_raw" ]]; then
      echo "[loop_iteration] missing reconcile control markers in $log_file" >&2
      echo "[loop_iteration] expected BEGIN_RECONCILE_CONTROL/END_RECONCILE_CONTROL with CLAIMABLE_WORK_EXISTS, ALL_WAITING_ON_OTHER_AGENTS, and SLEEP_NEXT_ITERATION" >&2
      exit 1
    fi

    claimable_work_exists="$(normalize_bool "$claimable_work_exists_raw")"
    waiting="$(normalize_bool "$waiting_raw")"
    sleep_next="$(normalize_bool "$sleep_next_raw")"

    if [[ "$sleep_next" == "no" && "$claimable_work_exists" == "no" && "$waiting" == "yes" ]]; then
      sleep_next="yes"
    fi
  else
    echo "[loop_iteration] missing supported control block in $log_file" >&2
    echo "[loop_iteration] expected either BEGIN_LOOP_CONTROL or BEGIN_RECONCILE_CONTROL" >&2
    exit 1
  fi

  write_iteration_json "$iteration" "$timestamp" "$log_file" "$json_file" "$control_mode"

  assert_primary_checkout_unchanged "$primary_branch_before" "$primary_head_before"
  cleanup_iteration_worktree "$iteration_worktree"

  if [[ "$control_mode" == "serialized" ]]; then
    echo "[loop_iteration] control_mode=serialized prs_processed=$prs_processed waiting_on_other_agents=$waiting sleep_next=$sleep_next"
  else
    echo "[loop_iteration] control_mode=reconcile claimable_work_exists=$claimable_work_exists waiting_on_other_agents=$waiting sleep_next=$sleep_next"
  fi
  echo "[loop_iteration] json: $json_file"

  if [[ $iteration -lt MAX_ITERATIONS && "$sleep_next" == "yes" ]]; then
    echo "[loop_iteration] sleeping for $WAIT_SECONDS seconds before next iteration"
    sleep "$WAIT_SECONDS"
  fi
done

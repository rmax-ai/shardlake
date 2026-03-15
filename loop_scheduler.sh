#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR"
RECONCILE_PROMPT_PATH="${LOOP_RECONCILE_PROMPT_PATH:-.github/prompts/loop_reconcile.prompt.md}"
MAX_CYCLES="${MAX_CYCLES:-100}"
WAIT_SECONDS="${WAIT_SECONDS:-300}"
RUN_DRAFT_REVIEW="${RUN_DRAFT_REVIEW:-yes}"
RUN_OPEN_REVIEW="${RUN_OPEN_REVIEW:-yes}"
RUN_MERGE="${RUN_MERGE:-yes}"
RUN_RECONCILE="${RUN_RECONCILE:-yes}"
COPILOT_BIN="${COPILOT_BIN:-copilot}"
LOG_DIR="${LOOP_SCHEDULER_LOG_DIR:-$REPO_ROOT/tmp/loop_scheduler}"

usage() {
  cat >&2 <<'EOF'
usage: loop_scheduler.sh [--once] [--max-cycles <count>] [--wait-seconds <seconds>] [--skip-reconcile] [--skip-draft-review] [--skip-open-review] [--skip-merge]

Runs the concurrent local loop as:
  1. one reconcile pass unless --skip-reconcile is set
  2. zero or more worker lanes
  3. optional sleep and repeat

Draft-review and open-review lanes may run concurrently. Merge remains single-lane.
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

normalize_bool() {
  local value="${1:-}"
  value="$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')"
  if [[ "$value" == "yes" || "$value" == "true" || "$value" == "1" ]]; then
    printf 'yes\n'
  else
    printf 'no\n'
  fi
}

json_field() {
  local file_path="$1"
  local field_path="$2"

  python3 - "$file_path" "$field_path" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
value = payload
for part in sys.argv[2].split('.'):
    if isinstance(value, dict):
        value = value.get(part)
    else:
        value = None
    if value is None:
        break

if value is None:
    print("")
elif isinstance(value, bool):
    print("true" if value else "false")
else:
    print(value)
PY
}

skip_reconcile_cycle() {
  local cycle="$1"

  CLAIMABLE_WORK_EXISTS="yes"
  ALL_WAITING_ON_OTHER_AGENTS="no"
  SLEEP_NEXT_ITERATION="yes"

  echo "[loop_scheduler] skipping reconcile cycle ${cycle}/${MAX_CYCLES}; dispatching workers directly"
}

run_reconcile_cycle() {
  local cycle="$1"
  local reconcile_output
  local reconcile_status=0
  local json_file

  echo "[loop_scheduler] starting reconcile cycle ${cycle}/${MAX_CYCLES}"
  set +e
  reconcile_output="$(
    cd "$REPO_ROOT" && \
    COPILOT_BIN="$COPILOT_BIN" LOOP_PROMPT_PATH="$RECONCILE_PROMPT_PATH" MAX_ITERATIONS=1 ./loop_iteration.sh 2>&1
  )"
  reconcile_status=$?
  set -e
  printf '%s\n' "$reconcile_output"

  if [[ "$reconcile_status" -ne 0 ]]; then
    echo "[loop_scheduler] reconcile cycle ${cycle} failed" >&2
    exit "$reconcile_status"
  fi

  json_file="$(printf '%s\n' "$reconcile_output" | awk '/^\[loop_iteration\] json: / { print $3 }' | tail -n 1)"
  if [[ -z "$json_file" || ! -f "$json_file" ]]; then
    echo "[loop_scheduler] could not resolve reconcile json artifact from cycle ${cycle}" >&2
    exit 1
  fi

  CLAIMABLE_WORK_EXISTS="$(normalize_bool "$(json_field "$json_file" control.claimable_work_exists)")"
  ALL_WAITING_ON_OTHER_AGENTS="$(normalize_bool "$(json_field "$json_file" control.all_waiting_on_other_agents)")"
  SLEEP_NEXT_ITERATION="$(normalize_bool "$(json_field "$json_file" control.sleep_next_iteration)")"

  echo "[loop_scheduler] reconcile json: $json_file"
  echo "[loop_scheduler] reconcile control: claimable_work_exists=$CLAIMABLE_WORK_EXISTS all_waiting_on_other_agents=$ALL_WAITING_ON_OTHER_AGENTS sleep_next_iteration=$SLEEP_NEXT_ITERATION"
}

launch_worker() {
  local lane="$1"
  local log_file="$2"

  echo "[loop_scheduler] launching worker lane=${lane}" >&2
  (
    cd "$REPO_ROOT"
    COPILOT_BIN="$COPILOT_BIN" ./loop_worker.sh --lane "$lane"
  ) >"$log_file" 2>&1 &
  printf '%s\n' "$!"
}

wait_for_worker() {
  local lane="$1"
  local pid="$2"
  local log_file="$3"

  if wait "$pid"; then
    echo "[loop_scheduler] worker lane=${lane} completed successfully"
    return 0
  fi

  status=$?
  echo "[loop_scheduler] worker lane=${lane} failed with status ${status}; log=${log_file}" >&2
  sed -n '1,200p' "$log_file" >&2 || true
  exit "$status"
}

dispatch_workers() {
  local cycle="$1"
  local draft_pid=""
  local draft_log=""
  local open_pid=""
  local open_log=""
  local merge_log=""

  if [[ "$CLAIMABLE_WORK_EXISTS" != "yes" ]]; then
    echo "[loop_scheduler] skipping worker dispatch because no claimable work exists"
    return
  fi

  mkdir -p "$LOG_DIR"

  if [[ "$RUN_DRAFT_REVIEW" == "yes" ]]; then
    draft_log="$LOG_DIR/cycle_${cycle}_draft-review.log"
    draft_pid="$(launch_worker draft-review "$draft_log")"
  fi

  if [[ "$RUN_OPEN_REVIEW" == "yes" ]]; then
    open_log="$LOG_DIR/cycle_${cycle}_open-review.log"
    open_pid="$(launch_worker open-review "$open_log")"
  fi

  if [[ -n "$draft_pid" ]]; then
    wait_for_worker draft-review "$draft_pid" "$draft_log"
  fi

  if [[ -n "$open_pid" ]]; then
    wait_for_worker open-review "$open_pid" "$open_log"
  fi

  if [[ "$RUN_MERGE" == "yes" ]]; then
    merge_log="$LOG_DIR/cycle_${cycle}_merge.log"
    echo "[loop_scheduler] launching worker lane=merge"
    (
      cd "$REPO_ROOT"
      COPILOT_BIN="$COPILOT_BIN" ./loop_worker.sh --lane merge
    ) >"$merge_log" 2>&1
    echo "[loop_scheduler] worker lane=merge completed successfully"
  fi
}

ONCE="no"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --once)
      ONCE="yes"
      MAX_CYCLES=1
      shift
      ;;
    --max-cycles)
      MAX_CYCLES="$2"
      shift 2
      ;;
    --wait-seconds)
      WAIT_SECONDS="$2"
      shift 2
      ;;
    --skip-reconcile)
      RUN_RECONCILE="no"
      shift
      ;;
    --skip-draft-review)
      RUN_DRAFT_REVIEW="no"
      shift
      ;;
    --skip-open-review)
      RUN_OPEN_REVIEW="no"
      shift
      ;;
    --skip-merge)
      RUN_MERGE="no"
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

if ! [[ "$MAX_CYCLES" =~ ^[1-9][0-9]*$ ]]; then
  die "max-cycles must be a positive integer"
fi

if ! [[ "$WAIT_SECONDS" =~ ^[0-9]+$ ]]; then
  die "wait-seconds must be a non-negative integer"
fi

RUN_DRAFT_REVIEW="$(normalize_bool "$RUN_DRAFT_REVIEW")"
RUN_OPEN_REVIEW="$(normalize_bool "$RUN_OPEN_REVIEW")"
RUN_MERGE="$(normalize_bool "$RUN_MERGE")"
RUN_RECONCILE="$(normalize_bool "$RUN_RECONCILE")"

if [[ "$RUN_RECONCILE" == "yes" || "$RUN_DRAFT_REVIEW" == "yes" || "$RUN_OPEN_REVIEW" == "yes" || "$RUN_MERGE" == "yes" ]]; then
  require_command "$COPILOT_BIN"
  require_command git
  require_command gh
fi

if [[ "$RUN_RECONCILE" == "yes" ]]; then
  require_command python3
  require_command awk
fi

if [[ "$RUN_RECONCILE" == "yes" && ! -f "$REPO_ROOT/$RECONCILE_PROMPT_PATH" ]]; then
  die "reconcile prompt file not found: $RECONCILE_PROMPT_PATH"
fi

mkdir -p "$LOG_DIR"
cd "$REPO_ROOT"

echo "[loop_scheduler] reconcile prompt: $RECONCILE_PROMPT_PATH"
echo "[loop_scheduler] max_cycles: $MAX_CYCLES"
echo "[loop_scheduler] wait_seconds: $WAIT_SECONDS"
echo "[loop_scheduler] run_reconcile: $RUN_RECONCILE"
echo "[loop_scheduler] run_draft_review: $RUN_DRAFT_REVIEW"
echo "[loop_scheduler] run_open_review: $RUN_OPEN_REVIEW"
echo "[loop_scheduler] run_merge: $RUN_MERGE"

for ((cycle = 1; cycle <= MAX_CYCLES; cycle++)); do
  if [[ "$RUN_RECONCILE" == "yes" ]]; then
    run_reconcile_cycle "$cycle"
  else
    skip_reconcile_cycle "$cycle"
  fi
  dispatch_workers "$cycle"

  if [[ "$ONCE" == "yes" || "$cycle" -eq "$MAX_CYCLES" ]]; then
    break
  fi

  if [[ "$SLEEP_NEXT_ITERATION" == "yes" ]]; then
    echo "[loop_scheduler] sleeping for ${WAIT_SECONDS} seconds before cycle $((cycle + 1))"
    sleep "$WAIT_SECONDS"
  fi
done

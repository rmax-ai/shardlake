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
RUN_CONFLICT_RESOLUTION="${RUN_CONFLICT_RESOLUTION:-yes}"
RUN_RECONCILE="${RUN_RECONCILE:-yes}"
DRAIN_LANES="${DRAIN_LANES:-no}"
COPILOT_BIN="${COPILOT_BIN:-copilot}"
LOG_DIR="${LOOP_SCHEDULER_LOG_DIR:-$REPO_ROOT/tmp/loop_scheduler}"
WORKER_NO_CANDIDATE_EXIT_STATUS="${LOOP_WORKER_NO_CANDIDATE_EXIT_STATUS:-10}"

usage() {
  cat >&2 <<'EOF'
usage: loop_scheduler.sh [--once] [--max-cycles <count>] [--wait-seconds <seconds>] [--drain-lanes] [--skip-reconcile] [--skip-draft-review] [--skip-open-review] [--skip-merge] [--skip-conflict-resolution]

Runs the concurrent local loop as:
  1. one reconcile pass unless --skip-reconcile is set
  2. zero or more worker lanes
  3. optional sleep and repeat

Draft-review and open-review lanes may run concurrently. Merge and conflict-resolve remain single-lane.
When --drain-lanes is set, each enabled lane keeps launching workers until that lane reports no remaining eligible PRs.
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

queue_has_work() {
  local file_path="$1"
  local queue_name="$2"

  python3 - "$file_path" "$queue_name" <<'PY'
import json
import re
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
queue_name = sys.argv[2]
body = (((payload.get("sections") or {}).get("worker_queues") or {}).get("body") or "")
pattern = re.compile(rf"^\s*(?:[-*]\s*)?{re.escape(queue_name)} queue:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE)
match = pattern.search(body)

if not match:
    print("unknown")
    raise SystemExit(0)

value = match.group(1).strip().lower()
if value in {"", "none", "(none)", "[]", "empty", "no", "no claimable prs", "no claimable pr"}:
    print("no")
else:
    print("yes")
PY
}

normalize_queue_flag() {
  local value="$1"
  local fallback="$2"

  case "$value" in
    yes|no)
      printf '%s\n' "$value"
      ;;
    *)
      printf '%s\n' "$fallback"
      ;;
  esac
}

skip_reconcile_cycle() {
  local cycle="$1"

  CLAIMABLE_WORK_EXISTS="yes"
  ALL_WAITING_ON_OTHER_AGENTS="no"
  SLEEP_NEXT_ITERATION="yes"
  DRAFT_REVIEW_QUEUE_EXISTS="yes"
  OPEN_REVIEW_QUEUE_EXISTS="yes"
  MERGE_QUEUE_EXISTS="yes"
  CONFLICT_RESOLVE_QUEUE_EXISTS="yes"

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

  if [[ "$CLAIMABLE_WORK_EXISTS" == "yes" ]]; then
    DRAFT_REVIEW_QUEUE_EXISTS="$(normalize_queue_flag "$(queue_has_work "$json_file" draft-review)" yes)"
    OPEN_REVIEW_QUEUE_EXISTS="$(normalize_queue_flag "$(queue_has_work "$json_file" open-review)" yes)"
    MERGE_QUEUE_EXISTS="$(normalize_queue_flag "$(queue_has_work "$json_file" merge)" yes)"
    CONFLICT_RESOLVE_QUEUE_EXISTS="$(normalize_queue_flag "$(queue_has_work "$json_file" conflict-resolve)" yes)"
  else
    DRAFT_REVIEW_QUEUE_EXISTS="no"
    OPEN_REVIEW_QUEUE_EXISTS="no"
    MERGE_QUEUE_EXISTS="no"
    CONFLICT_RESOLVE_QUEUE_EXISTS="no"
  fi

  echo "[loop_scheduler] reconcile json: $json_file"
  echo "[loop_scheduler] reconcile control: claimable_work_exists=$CLAIMABLE_WORK_EXISTS all_waiting_on_other_agents=$ALL_WAITING_ON_OTHER_AGENTS sleep_next_iteration=$SLEEP_NEXT_ITERATION"
  echo "[loop_scheduler] queue availability: draft-review=$DRAFT_REVIEW_QUEUE_EXISTS open-review=$OPEN_REVIEW_QUEUE_EXISTS merge=$MERGE_QUEUE_EXISTS conflict-resolve=$CONFLICT_RESOLVE_QUEUE_EXISTS"
}

launch_worker() {
  local lane="$1"
  local log_file="$2"

  echo "[loop_scheduler] launching worker lane=${lane}" >&2
  (
    cd "$REPO_ROOT"
    COPILOT_BIN="$COPILOT_BIN" ./loop_worker.sh --lane "$lane"
  ) >"$log_file" 2>&1 &
  LAUNCHED_WORKER_PID="$!"
}

wait_for_worker() {
  local lane="$1"
  local pid="$2"
  local log_file="$3"
  local status
  local errexit_was_on="no"

  if [[ "$-" == *e* ]]; then
    errexit_was_on="yes"
    set +e
  fi
  wait "$pid"
  status=$?
  if [[ "$errexit_was_on" == "yes" ]]; then
    set -e
  fi

  if [[ "$status" -eq 0 ]]; then
    echo "[loop_scheduler] worker lane=${lane} completed successfully"
    WAITED_WORKER_STATUS="$status"
    return 0
  fi

  if [[ "$status" -eq "$WORKER_NO_CANDIDATE_EXIT_STATUS" ]]; then
    echo "[loop_scheduler] worker lane=${lane} reported no remaining eligible work"
    WAITED_WORKER_STATUS="$status"
    return 0
  fi

  echo "[loop_scheduler] worker lane=${lane} failed with status ${status}; log=${log_file}" >&2
  sed -n '1,200p' "$log_file" >&2 || true
  exit "$status"
}

run_worker_once() {
  local lane="$1"
  local log_file="$2"
  local status
  local errexit_was_on="no"

  echo "[loop_scheduler] launching worker lane=${lane}"
  if [[ "$-" == *e* ]]; then
    errexit_was_on="yes"
    set +e
  fi
  (
    cd "$REPO_ROOT"
    COPILOT_BIN="$COPILOT_BIN" ./loop_worker.sh --lane "$lane"
  ) >"$log_file" 2>&1
  status=$?
  if [[ "$errexit_was_on" == "yes" ]]; then
    set -e
  fi

  if [[ "$status" -eq 0 ]]; then
    echo "[loop_scheduler] worker lane=${lane} completed successfully"
    return 0
  fi

  if [[ "$status" -eq "$WORKER_NO_CANDIDATE_EXIT_STATUS" ]]; then
    echo "[loop_scheduler] worker lane=${lane} reported no remaining eligible work"
    return "$status"
  fi

  echo "[loop_scheduler] worker lane=${lane} failed with status ${status}; log=${log_file}" >&2
  sed -n '1,200p' "$log_file" >&2 || true
  exit "$status"
}

drain_review_lanes() {
  local cycle="$1"
  local draft_active="no"
  local open_active="no"
  local attempt=1
  local draft_pid=""
  local draft_log=""
  local open_pid=""
  local open_log=""

  if [[ "$RUN_DRAFT_REVIEW" == "yes" && "$DRAFT_REVIEW_QUEUE_EXISTS" == "yes" ]]; then
    draft_active="yes"
  elif [[ "$RUN_DRAFT_REVIEW" == "yes" ]]; then
    echo "[loop_scheduler] skipping worker lane=draft-review because the reconciler reported no queued work"
  fi

  if [[ "$RUN_OPEN_REVIEW" == "yes" && "$OPEN_REVIEW_QUEUE_EXISTS" == "yes" ]]; then
    open_active="yes"
  elif [[ "$RUN_OPEN_REVIEW" == "yes" ]]; then
    echo "[loop_scheduler] skipping worker lane=open-review because the reconciler reported no queued work"
  fi

  while [[ "$draft_active" == "yes" || "$open_active" == "yes" ]]; do
    draft_pid=""
    draft_log=""
    open_pid=""
    open_log=""

    if [[ "$draft_active" == "yes" ]]; then
      draft_log="$LOG_DIR/cycle_${cycle}_drain${attempt}_draft-review.log"
      launch_worker draft-review "$draft_log"
      draft_pid="$LAUNCHED_WORKER_PID"
    fi

    if [[ "$open_active" == "yes" ]]; then
      open_log="$LOG_DIR/cycle_${cycle}_drain${attempt}_open-review.log"
      launch_worker open-review "$open_log"
      open_pid="$LAUNCHED_WORKER_PID"
    fi

    if [[ -n "$draft_pid" ]]; then
      wait_for_worker draft-review "$draft_pid" "$draft_log"
      if [[ "$WAITED_WORKER_STATUS" -eq "$WORKER_NO_CANDIDATE_EXIT_STATUS" ]]; then
        draft_active="no"
      fi
    fi

    if [[ -n "$open_pid" ]]; then
      wait_for_worker open-review "$open_pid" "$open_log"
      if [[ "$WAITED_WORKER_STATUS" -eq "$WORKER_NO_CANDIDATE_EXIT_STATUS" ]]; then
        open_active="no"
      fi
    fi

    attempt=$((attempt + 1))
  done
}

drain_single_lane() {
  local cycle="$1"
  local lane="$2"
  local enabled="$3"
  local queue_exists="$4"
  local attempt=1
  local log_file
  local status

  if [[ "$enabled" != "yes" ]]; then
    return
  fi

  if [[ "$queue_exists" != "yes" ]]; then
    echo "[loop_scheduler] skipping worker lane=${lane} because the reconciler reported no queued work"
    return
  fi

  while true; do
    log_file="$LOG_DIR/cycle_${cycle}_drain${attempt}_${lane}.log"
    set +e
    run_worker_once "$lane" "$log_file"
    status=$?
    set -e

    if [[ "$status" -eq "$WORKER_NO_CANDIDATE_EXIT_STATUS" ]]; then
      return 0
    fi

    attempt=$((attempt + 1))
  done
}

dispatch_workers() {
  local cycle="$1"
  local draft_pid=""
  local draft_log=""
  local open_pid=""
  local open_log=""
  local merge_log=""
  local conflict_log=""

  if [[ "$CLAIMABLE_WORK_EXISTS" != "yes" ]]; then
    echo "[loop_scheduler] skipping worker dispatch because no claimable work exists"
    return
  fi

  mkdir -p "$LOG_DIR"

  if [[ "$DRAIN_LANES" == "yes" ]]; then
    drain_review_lanes "$cycle"
    drain_single_lane "$cycle" merge "$RUN_MERGE" "$MERGE_QUEUE_EXISTS"
    drain_single_lane "$cycle" conflict-resolve "$RUN_CONFLICT_RESOLUTION" "$CONFLICT_RESOLVE_QUEUE_EXISTS"
    return
  fi

  if [[ "$RUN_DRAFT_REVIEW" == "yes" && "$DRAFT_REVIEW_QUEUE_EXISTS" == "yes" ]]; then
    draft_log="$LOG_DIR/cycle_${cycle}_draft-review.log"
    launch_worker draft-review "$draft_log"
    draft_pid="$LAUNCHED_WORKER_PID"
  elif [[ "$RUN_DRAFT_REVIEW" == "yes" ]]; then
    echo "[loop_scheduler] skipping worker lane=draft-review because the reconciler reported no queued work"
  fi

  if [[ "$RUN_OPEN_REVIEW" == "yes" && "$OPEN_REVIEW_QUEUE_EXISTS" == "yes" ]]; then
    open_log="$LOG_DIR/cycle_${cycle}_open-review.log"
    launch_worker open-review "$open_log"
    open_pid="$LAUNCHED_WORKER_PID"
  elif [[ "$RUN_OPEN_REVIEW" == "yes" ]]; then
    echo "[loop_scheduler] skipping worker lane=open-review because the reconciler reported no queued work"
  fi

  if [[ -n "$draft_pid" ]]; then
    wait_for_worker draft-review "$draft_pid" "$draft_log"
  fi

  if [[ -n "$open_pid" ]]; then
    wait_for_worker open-review "$open_pid" "$open_log"
  fi

  if [[ "$RUN_MERGE" == "yes" && "$MERGE_QUEUE_EXISTS" == "yes" ]]; then
    merge_log="$LOG_DIR/cycle_${cycle}_merge.log"
    set +e
    run_worker_once merge "$merge_log"
    set -e
  elif [[ "$RUN_MERGE" == "yes" ]]; then
    echo "[loop_scheduler] skipping worker lane=merge because the reconciler reported no queued work"
  fi

  if [[ "$RUN_CONFLICT_RESOLUTION" == "yes" && "$CONFLICT_RESOLVE_QUEUE_EXISTS" == "yes" ]]; then
    conflict_log="$LOG_DIR/cycle_${cycle}_conflict-resolve.log"
    set +e
    run_worker_once conflict-resolve "$conflict_log"
    set -e
  elif [[ "$RUN_CONFLICT_RESOLUTION" == "yes" ]]; then
    echo "[loop_scheduler] skipping worker lane=conflict-resolve because the reconciler reported no queued work"
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
    --drain-lanes)
      DRAIN_LANES="yes"
      shift
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
    --skip-conflict-resolution)
      RUN_CONFLICT_RESOLUTION="no"
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
RUN_CONFLICT_RESOLUTION="$(normalize_bool "$RUN_CONFLICT_RESOLUTION")"
RUN_RECONCILE="$(normalize_bool "$RUN_RECONCILE")"
DRAIN_LANES="$(normalize_bool "$DRAIN_LANES")"

if [[ "$RUN_RECONCILE" == "yes" || "$RUN_DRAFT_REVIEW" == "yes" || "$RUN_OPEN_REVIEW" == "yes" || "$RUN_MERGE" == "yes" || "$RUN_CONFLICT_RESOLUTION" == "yes" ]]; then
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
echo "[loop_scheduler] drain_lanes: $DRAIN_LANES"
echo "[loop_scheduler] run_draft_review: $RUN_DRAFT_REVIEW"
echo "[loop_scheduler] run_open_review: $RUN_OPEN_REVIEW"
echo "[loop_scheduler] run_merge: $RUN_MERGE"
echo "[loop_scheduler] run_conflict_resolution: $RUN_CONFLICT_RESOLUTION"

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

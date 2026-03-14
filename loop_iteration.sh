#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR"
PROMPT_PATH=".github/prompts/loop_iteration.prompt.md"
COPILOT_BIN="${COPILOT_BIN:-copilot}"
MAX_ITERATIONS="${MAX_ITERATIONS:-100}"
WAIT_SECONDS="${WAIT_SECONDS:-300}"
LOG_DIR="$REPO_ROOT/tmp/loop_iterations"
PROMPT_TEXT="follow instructions in ${PROMPT_PATH}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 127
  fi
}

extract_marker() {
  local marker="$1"
  local file="$2"

  awk -F': *' -v key="$marker" '$1 == key { value = $2 } END { print value }' "$file"
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

require_command "$COPILOT_BIN"
require_command tee
require_command awk
require_command sleep

mkdir -p "$LOG_DIR"

cd "$REPO_ROOT"

for ((iteration = 1; iteration <= MAX_ITERATIONS; iteration++)); do
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  log_file="$LOG_DIR/iteration_${iteration}_${timestamp}.log"

  echo "[loop_iteration] starting iteration ${iteration}/${MAX_ITERATIONS}"
  echo "[loop_iteration] log: $log_file"

  set +e
  "$COPILOT_BIN" --model gpt-5.4 --allow-all-tools -p "$PROMPT_TEXT" | tee "$log_file"
  command_status=${PIPESTATUS[0]}
  set -e

  if [[ $command_status -ne 0 ]]; then
    echo "[loop_iteration] copilot command failed with status $command_status" >&2
    exit "$command_status"
  fi

  prs_processed_raw="$(extract_marker "PRS_PROCESSED" "$log_file")"
  waiting_raw="$(extract_marker "ALL_WAITING_ON_OTHER_AGENTS" "$log_file")"
  sleep_next_raw="$(extract_marker "SLEEP_NEXT_ITERATION" "$log_file")"

  if [[ -z "$prs_processed_raw" || -z "$waiting_raw" || -z "$sleep_next_raw" ]]; then
    echo "[loop_iteration] missing control markers in $log_file" >&2
    echo "[loop_iteration] expected PRS_PROCESSED, ALL_WAITING_ON_OTHER_AGENTS, and SLEEP_NEXT_ITERATION" >&2
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

  echo "[loop_iteration] prs_processed=$prs_processed waiting_on_other_agents=$waiting sleep_next=$sleep_next"

  if [[ $iteration -lt MAX_ITERATIONS && "$sleep_next" == "yes" ]]; then
    echo "[loop_iteration] sleeping for $WAIT_SECONDS seconds before next iteration"
    sleep "$WAIT_SECONDS"
  fi
done

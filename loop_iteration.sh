#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR"
PROMPT_PATH=".github/prompts/loop_iteration.prompt.md"
CONTROL_PROMPT_PATH=".github/prompts/loop_control.prompt.md"
COPILOT_BIN="${COPILOT_BIN:-copilot}"
MAX_ITERATIONS="${MAX_ITERATIONS:-100}"
WAIT_SECONDS="${WAIT_SECONDS:-300}"
LOG_DIR="$REPO_ROOT/tmp/loop_iterations"
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

  awk -v key="$marker" '
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

      if (line == "BEGIN_LOOP_CONTROL") {
        in_block = 1
        block_seen = 1
        next
      }

      if (line == "END_LOOP_CONTROL") {
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

  set +e
  "$COPILOT_BIN" --model gpt-5.4 --allow-all-tools -p "$prompt_text" | tee "$output_file"
  local command_status=${PIPESTATUS[0]}
  set -e

  return "$command_status"
}

require_command "$COPILOT_BIN"
require_command tee
require_command awk
require_command sleep

mkdir -p "$LOG_DIR"

cd "$REPO_ROOT"

export GH_PAGER="$GH_PAGER_VALUE"
export NO_COLOR="$NO_COLOR_VALUE"
export CLICOLOR="$CLICOLOR_VALUE"

for ((iteration = 1; iteration <= MAX_ITERATIONS; iteration++)); do
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  log_file="$LOG_DIR/iteration_${iteration}_${timestamp}.log"
  control_file="$LOG_DIR/iteration_${iteration}_${timestamp}.control.log"

  echo "[loop_iteration] starting iteration ${iteration}/${MAX_ITERATIONS}"
  echo "[loop_iteration] log: $log_file"

  if ! run_prompt "follow instructions in ${PROMPT_PATH}" "$log_file"; then
    command_status=$?
  else
    command_status=0
  fi

  if [[ $command_status -ne 0 ]]; then
    echo "[loop_iteration] copilot command failed with status $command_status" >&2
    exit "$command_status"
  fi

  echo "[loop_iteration] synthesizing loop control from $log_file"

  if ! run_prompt "follow instructions in ${CONTROL_PROMPT_PATH} for log file ${log_file}" "$control_file"; then
    command_status=$?
  else
    command_status=0
  fi

  if [[ $command_status -ne 0 ]]; then
    echo "[loop_iteration] loop control synthesis failed with status $command_status" >&2
    exit "$command_status"
  fi

  cat "$control_file" >> "$log_file"

  prs_processed_raw="$(extract_marker "PRS_PROCESSED" "$control_file")"
  waiting_raw="$(extract_marker "ALL_WAITING_ON_OTHER_AGENTS" "$control_file")"
  sleep_next_raw="$(extract_marker "SLEEP_NEXT_ITERATION" "$control_file")"

  if [[ -z "$prs_processed_raw" || -z "$waiting_raw" || -z "$sleep_next_raw" ]]; then
    echo "[loop_iteration] missing control markers in $control_file" >&2
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

  echo "[loop_iteration] prs_processed=$prs_processed waiting_on_other_agents=$waiting sleep_next=$sleep_next"

  if [[ $iteration -lt MAX_ITERATIONS && "$sleep_next" == "yes" ]]; then
    echo "[loop_iteration] sleeping for $WAIT_SECONDS seconds before next iteration"
    sleep "$WAIT_SECONDS"
  fi
done

#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SOURCE_SCHEDULER="$REPO_ROOT/loop_scheduler.sh"

fail() {
  echo "[test_loop_scheduler] $*" >&2
  exit 1
}

assert_contains() {
  local haystack="$1"
  local needle="$2"

  if [[ "$haystack" != *"$needle"* ]]; then
    fail "expected output to contain: $needle"
  fi
}

assert_not_contains() {
  local haystack="$1"
  local needle="$2"

  if [[ "$haystack" == *"$needle"* ]]; then
    fail "did not expect output to contain: $needle"
  fi
}

assert_order() {
  local haystack="$1"
  local first="$2"
  local second="$3"
  local first_line
  local second_line

  first_line="$(printf '%s\n' "$haystack" | grep -nF "$first" | head -n 1 | cut -d: -f1)"
  second_line="$(printf '%s\n' "$haystack" | grep -nF "$second" | head -n 1 | cut -d: -f1)"

  if [[ -z "$first_line" || -z "$second_line" || "$first_line" -ge "$second_line" ]]; then
    fail "expected output order: '$first' before '$second'"
  fi
}

setup_sandbox() {
  local sandbox_root="$1"

  mkdir -p "$sandbox_root/bin" "$sandbox_root/state"
  cp "$SOURCE_SCHEDULER" "$sandbox_root/loop_scheduler.sh"
  chmod +x "$sandbox_root/loop_scheduler.sh"
  mkdir -p "$sandbox_root/.github/prompts"
  printf '%s\n' 'stub reconcile prompt' >"$sandbox_root/.github/prompts/loop_reconcile.prompt.md"

  cat >"$sandbox_root/loop_worker.sh" <<'EOF'
#!/usr/bin/env bash

set -euo pipefail

lane=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --lane)
      lane="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

echo "fake worker lane=${lane}"
sleep 0.1

sequence=""
case "$lane" in
  draft-review)
    sequence="${FAKE_DRAFT_SEQUENCE:-${FAKE_DRAFT_STATUS:-0}}"
    ;;
  open-review)
    sequence="${FAKE_OPEN_SEQUENCE:-${FAKE_OPEN_STATUS:-0}}"
    ;;
  merge)
    sequence="${FAKE_MERGE_SEQUENCE:-${FAKE_MERGE_STATUS:-0}}"
    ;;
  conflict-resolve)
    sequence="${FAKE_CONFLICT_SEQUENCE:-${FAKE_CONFLICT_STATUS:-0}}"
    ;;
  *)
    echo "unexpected lane: ${lane}" >&2
    exit 64
    ;;
esac

state_dir="${FAKE_WORKER_STATE_DIR:?}"
count_file="$state_dir/${lane}.count"
count=0
if [[ -f "$count_file" ]]; then
  count="$(<"$count_file")"
fi
count=$((count + 1))
printf '%s\n' "$count" >"$count_file"

IFS=',' read -r -a statuses <<<"$sequence"
index=$((count - 1))
if [[ "$index" -ge "${#statuses[@]}" ]]; then
  index=$((${#statuses[@]} - 1))
fi

exit "${statuses[$index]}"
EOF
  chmod +x "$sandbox_root/loop_worker.sh"

  cat >"$sandbox_root/loop_iteration.sh" <<'EOF'
#!/usr/bin/env bash

set -euo pipefail

json_file="$PWD/fake_reconcile.json"
cat >"$json_file" <<JSON
{
  "sections": {
    "worker_queues": {
      "body": "draft-review queue: ${FAKE_QUEUE_DRAFT:-#11}\nopen-review queue: ${FAKE_QUEUE_OPEN:-#12}\nmerge queue: ${FAKE_QUEUE_MERGE:-#13}\nconflict-resolve queue: ${FAKE_QUEUE_CONFLICT:-#14}"
    }
  },
  "control": {
    "claimable_work_exists": "${FAKE_RECONCILE_CLAIMABLE:-yes}",
    "all_waiting_on_other_agents": "${FAKE_RECONCILE_WAITING:-no}",
    "sleep_next_iteration": "${FAKE_RECONCILE_SLEEP:-no}"
  }
}
JSON

echo "[loop_iteration] json: $json_file"
EOF
  chmod +x "$sandbox_root/loop_iteration.sh"

  cat >"$sandbox_root/bin/copilot" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  chmod +x "$sandbox_root/bin/copilot"

  cat >"$sandbox_root/bin/git" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  chmod +x "$sandbox_root/bin/git"

  cat >"$sandbox_root/bin/gh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  chmod +x "$sandbox_root/bin/gh"
}

run_scheduler() {
  local sandbox_root="$1"
  shift

  (
    cd "$sandbox_root"
    PATH="$sandbox_root/bin:$PATH" \
    LOOP_SCHEDULER_LOG_DIR="$sandbox_root/logs" \
    FAKE_WORKER_STATE_DIR="$sandbox_root/state" \
    "$sandbox_root/loop_scheduler.sh" "$@"
  ) 2>&1
}

test_concurrent_workers_complete() (
  local sandbox_root
  local output

  sandbox_root="$(mktemp -d)"
  trap 'rm -rf "$sandbox_root"' EXIT
  setup_sandbox "$sandbox_root"

  output="$(run_scheduler "$sandbox_root" --once --skip-reconcile --skip-merge --skip-conflict-resolution)"
  assert_contains "$output" "launching worker lane=draft-review"
  assert_contains "$output" "launching worker lane=open-review"
  assert_contains "$output" "worker lane=draft-review completed successfully"
  assert_contains "$output" "worker lane=open-review completed successfully"
  assert_not_contains "$output" "not a child of this shell"
  assert_not_contains "$output" "failed with status 0"
)

test_worker_failure_reports_real_status() (
  local sandbox_root
  local output
  local status

  sandbox_root="$(mktemp -d)"
  trap 'rm -rf "$sandbox_root"' EXIT
  setup_sandbox "$sandbox_root"

  set +e
  output="$(FAKE_DRAFT_STATUS=7 run_scheduler "$sandbox_root" --once --skip-reconcile --skip-open-review --skip-merge --skip-conflict-resolution)"
  status=$?
  set -e

  if [[ "$status" -ne 7 ]]; then
    fail "expected scheduler to exit 7 for a failing draft-review worker, got ${status}"
  fi

  assert_contains "$output" "worker lane=draft-review failed with status 7"
  assert_not_contains "$output" "failed with status 0"
)

test_conflict_resolve_dispatch_when_enabled() (
  local sandbox_root
  local output

  sandbox_root="$(mktemp -d)"
  trap 'rm -rf "$sandbox_root"' EXIT
  setup_sandbox "$sandbox_root"

  output="$(FAKE_QUEUE_DRAFT=none FAKE_QUEUE_OPEN=none FAKE_QUEUE_MERGE=none FAKE_QUEUE_CONFLICT='#44' run_scheduler "$sandbox_root" --once)"
  assert_contains "$output" "queue availability: draft-review=no open-review=no merge=no conflict-resolve=yes"
  assert_contains "$output" "launching worker lane=conflict-resolve"
  assert_contains "$output" "worker lane=conflict-resolve completed successfully"
)

test_skip_conflict_resolution_flag() (
  local sandbox_root
  local output

  sandbox_root="$(mktemp -d)"
  trap 'rm -rf "$sandbox_root"' EXIT
  setup_sandbox "$sandbox_root"

  output="$(FAKE_QUEUE_DRAFT=none FAKE_QUEUE_OPEN=none FAKE_QUEUE_MERGE=none FAKE_QUEUE_CONFLICT='#44' run_scheduler "$sandbox_root" --once --skip-conflict-resolution)"
  assert_contains "$output" "run_conflict_resolution: no"
  assert_not_contains "$output" "launching worker lane=conflict-resolve"
)

test_dispatch_ordering_after_merge() (
  local sandbox_root
  local output

  sandbox_root="$(mktemp -d)"
  trap 'rm -rf "$sandbox_root"' EXIT
  setup_sandbox "$sandbox_root"

  output="$(run_scheduler "$sandbox_root" --once --skip-reconcile --skip-draft-review --skip-open-review)"
  assert_order "$output" "launching worker lane=merge" "worker lane=merge completed successfully"
  assert_order "$output" "worker lane=merge completed successfully" "launching worker lane=conflict-resolve"
)

test_drain_lanes_repeats_until_empty() (
  local sandbox_root
  local output

  sandbox_root="$(mktemp -d)"
  trap 'rm -rf "$sandbox_root"' EXIT
  setup_sandbox "$sandbox_root"

  output="$(FAKE_DRAFT_SEQUENCE='0,10' FAKE_OPEN_SEQUENCE='0,0,10' FAKE_MERGE_SEQUENCE='0,10' FAKE_CONFLICT_SEQUENCE='10' run_scheduler "$sandbox_root" --once --skip-reconcile --drain-lanes)"
  assert_contains "$output" "drain_lanes: yes"
  assert_contains "$output" "worker lane=draft-review reported no remaining eligible work"
  assert_contains "$output" "worker lane=open-review reported no remaining eligible work"
  assert_contains "$output" "worker lane=merge reported no remaining eligible work"
  assert_contains "$output" "worker lane=conflict-resolve reported no remaining eligible work"
  assert_contains "$output" "launching worker lane=draft-review"
  assert_contains "$output" "launching worker lane=open-review"
  assert_contains "$output" "launching worker lane=merge"
)

test_no_work_status_is_not_a_failure_without_drain() (
  local sandbox_root
  local output

  sandbox_root="$(mktemp -d)"
  trap 'rm -rf "$sandbox_root"' EXIT
  setup_sandbox "$sandbox_root"

  output="$(FAKE_MERGE_STATUS=10 run_scheduler "$sandbox_root" --once --skip-reconcile --skip-draft-review --skip-open-review --skip-conflict-resolution)"
  assert_contains "$output" "worker lane=merge reported no remaining eligible work"
  assert_not_contains "$output" "worker lane=merge failed"
)

test_no_conflict_dispatch_without_conflicted_queue() (
  local sandbox_root
  local output

  sandbox_root="$(mktemp -d)"
  trap 'rm -rf "$sandbox_root"' EXIT
  setup_sandbox "$sandbox_root"

  output="$(FAKE_QUEUE_DRAFT='#11' FAKE_QUEUE_OPEN=none FAKE_QUEUE_MERGE=none FAKE_QUEUE_CONFLICT=none run_scheduler "$sandbox_root" --once)"
  assert_contains "$output" "launching worker lane=draft-review"
  assert_contains "$output" "skipping worker lane=conflict-resolve because the reconciler reported no queued work"
  assert_not_contains "$output" "launching worker lane=conflict-resolve"
)

main() {
  test_concurrent_workers_complete
  test_worker_failure_reports_real_status
  test_conflict_resolve_dispatch_when_enabled
  test_skip_conflict_resolution_flag
  test_dispatch_ordering_after_merge
  test_drain_lanes_repeats_until_empty
  test_no_work_status_is_not_a_failure_without_drain
  test_no_conflict_dispatch_without_conflicted_queue
  echo "[test_loop_scheduler] PASS"
}

main "$@"

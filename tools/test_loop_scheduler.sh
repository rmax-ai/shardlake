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

setup_sandbox() {
  local sandbox_root="$1"

  mkdir -p "$sandbox_root/bin"
  cp "$SOURCE_SCHEDULER" "$sandbox_root/loop_scheduler.sh"
  chmod +x "$sandbox_root/loop_scheduler.sh"

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

case "$lane" in
  draft-review)
    exit "${FAKE_DRAFT_STATUS:-0}"
    ;;
  open-review)
    exit "${FAKE_OPEN_STATUS:-0}"
    ;;
  merge)
    exit "${FAKE_MERGE_STATUS:-0}"
    ;;
  *)
    echo "unexpected lane: ${lane}" >&2
    exit 64
    ;;
esac
EOF
  chmod +x "$sandbox_root/loop_worker.sh"

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
    "$sandbox_root/loop_scheduler.sh" "$@"
  ) 2>&1
}

test_concurrent_workers_complete() (
  local sandbox_root
  local output

  sandbox_root="$(mktemp -d)"
  trap 'rm -rf "$sandbox_root"' EXIT
  setup_sandbox "$sandbox_root"

  output="$(run_scheduler "$sandbox_root" --once --skip-reconcile --skip-merge)"
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
  output="$(FAKE_DRAFT_STATUS=7 run_scheduler "$sandbox_root" --once --skip-reconcile --skip-open-review --skip-merge)"
  status=$?
  set -e

  if [[ "$status" -ne 7 ]]; then
    fail "expected scheduler to exit 7 for a failing draft-review worker, got ${status}"
  fi

  assert_contains "$output" "worker lane=draft-review failed with status 7"
  assert_not_contains "$output" "failed with status 0"
)

main() {
  test_concurrent_workers_complete
  test_worker_failure_reports_real_status
  echo "[test_loop_scheduler] PASS"
}

main "$@"

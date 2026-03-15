#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SOURCE_WORKER="$REPO_ROOT/loop_worker.sh"

fail() {
  echo "[test_loop_worker] $*" >&2
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

assert_file_contains() {
  local file_path="$1"
  local needle="$2"
  local content

  content="$(<"$file_path")"
  assert_contains "$content" "$needle"
}

setup_sandbox() {
  local sandbox_root="$1"

  mkdir -p "$sandbox_root/bin" "$sandbox_root/tools" "$sandbox_root/.github/prompts"
  cp "$SOURCE_WORKER" "$sandbox_root/loop_worker.sh"
  chmod +x "$sandbox_root/loop_worker.sh"

  for prompt in \
    worker-review-draft-pr.prompt.md \
    worker-review-open-pr.prompt.md \
    worker-merge-pr.prompt.md \
    worker-conflict-resolve-pr.prompt.md
  do
    printf '%s\n' 'stub prompt' >"$sandbox_root/.github/prompts/$prompt"
  done

  cat >"$sandbox_root/tools/loop_claim.sh" <<'EOF'
#!/usr/bin/env bash

set -euo pipefail

command="$1"
shift

case "$command" in
  acquire)
    echo '{"lease_ref":"refs/heads/loop-claims/test/pr-7","status":"acquired"}'
    ;;
  renew)
    echo '{"status":"renewed"}'
    ;;
  release)
    echo '{"status":"released"}'
    ;;
  inspect)
    echo '{"status":"active","owner":"test-owner","expected_head_sha":"ignored"}'
    ;;
  *)
    echo "unexpected claim command: $command" >&2
    exit 64
    ;;
esac
EOF
  chmod +x "$sandbox_root/tools/loop_claim.sh"

  cat >"$sandbox_root/bin/copilot" <<'EOF'
#!/usr/bin/env bash

echo "fake copilot"
exit 0
EOF
  chmod +x "$sandbox_root/bin/copilot"

  cat >"$sandbox_root/bin/gh" <<'EOF'
#!/usr/bin/env bash

set -euo pipefail

if [[ "$1" == "auth" && "$2" == "status" ]]; then
  exit 0
fi

if [[ "$1" == "repo" && "$2" == "view" ]]; then
  echo '{"nameWithOwner":"rmax-ai/shardlake"}'
  exit 0
fi

if [[ "$1" == "pr" && "$2" == "view" ]]; then
  printf '%s\n' "$FAKE_PR_JSON"
  exit 0
fi

if [[ "$1" == "pr" && "$2" == "list" ]]; then
  printf '[%s]\n' "$FAKE_PR_JSON"
  exit 0
fi

echo "unexpected gh invocation: $*" >&2
exit 64
EOF
  chmod +x "$sandbox_root/bin/gh"

  git -C "$sandbox_root" init -b main >/dev/null
  git -C "$sandbox_root" config user.name tester
  git -C "$sandbox_root" config user.email tester@example.com
  printf '%s\n' 'sandbox' >"$sandbox_root/README.md"
  cat >"$sandbox_root/.gitignore" <<'EOF'
origin.git/
tmp/
EOF
  git -C "$sandbox_root" add . >/dev/null
  git -C "$sandbox_root" commit -m "init" >/dev/null

  git init --bare "$sandbox_root/origin.git" >/dev/null
  git -C "$sandbox_root" remote add origin "$sandbox_root/origin.git"
  git -C "$sandbox_root" push -u origin main >/dev/null
}

build_pr_json() {
  local pr_number="$1"
  local is_draft="$2"
  local labels_csv="$3"
  local head_sha="$4"

  python3 - "$pr_number" "$is_draft" "$labels_csv" "$head_sha" <<'PY'
import json
import sys

pr_number = int(sys.argv[1])
is_draft = sys.argv[2] == "true"
labels_csv = sys.argv[3]
head_sha = sys.argv[4]
labels = [{"name": label} for label in labels_csv.split(",") if label]

payload = {
    "number": pr_number,
    "title": f"PR {pr_number}",
    "isDraft": is_draft,
    "labels": labels,
    "headRefOid": head_sha,
    "baseRefName": "main",
    "author": {"login": "copilot-swe-agent"},
    "state": "OPEN",
    "url": f"https://example.test/pr/{pr_number}",
}
print(json.dumps(payload))
PY
}

run_worker() {
  local sandbox_root="$1"
  local lane="$2"
  local labels_csv="$3"
  local is_draft="$4"
  local pr_number="${5:-7}"
  local output

  output="$({
    cd "$sandbox_root"
    PATH="$sandbox_root/bin:$PATH" \
    FAKE_PR_JSON="$(build_pr_json "$pr_number" "$is_draft" "$labels_csv" "$(git -C "$sandbox_root" rev-parse HEAD)")" \
    WORKER_OWNER_ID="test-owner" \
    ./loop_worker.sh --lane "$lane" --pr "$pr_number"
  } 2>&1)"
  printf '%s\n' "$output"
}

test_conflict_pr_is_only_claimable_in_conflict_lane() (
  local sandbox_root
  local output

  sandbox_root="$(mktemp -d)"
  trap 'rm -rf "$sandbox_root"' EXIT
  setup_sandbox "$sandbox_root"

  output="$(run_worker "$sandbox_root" conflict-resolve 'has-merge-conflicts' false)"
  assert_contains "$output" "considering PR #7"
  assert_contains "$output" "worker completed for PR #7"

  output="$(run_worker "$sandbox_root" open-review 'has-merge-conflicts,ready-for-open-review' false)"
  assert_contains "$output" "target PR #7 is not currently eligible for lane open-review"
  assert_not_contains "$output" "considering PR #7"

  output="$(run_worker "$sandbox_root" merge 'has-merge-conflicts,ready-to-merge' false)"
  assert_contains "$output" "target PR #7 is not currently eligible for lane merge"
  assert_not_contains "$output" "considering PR #7"
)

test_conflict_pr_with_needs_human_is_not_eligible() (
  local sandbox_root
  local output

  sandbox_root="$(mktemp -d)"
  trap 'rm -rf "$sandbox_root"' EXIT
  setup_sandbox "$sandbox_root"

  output="$(run_worker "$sandbox_root" conflict-resolve 'has-merge-conflicts,needs-human' false)"
  assert_contains "$output" "target PR #7 is not currently eligible for lane conflict-resolve"
  assert_not_contains "$output" "considering PR #7"
)

test_non_conflicted_pr_is_not_eligible_for_conflict_lane() (
  local sandbox_root
  local output

  sandbox_root="$(mktemp -d)"
  trap 'rm -rf "$sandbox_root"' EXIT
  setup_sandbox "$sandbox_root"

  output="$(run_worker "$sandbox_root" conflict-resolve 'ready-to-merge' false)"
  assert_contains "$output" "target PR #7 is not currently eligible for lane conflict-resolve"
  assert_not_contains "$output" "considering PR #7"
)

test_conflict_lane_rejects_draft_prs() (
  local sandbox_root
  local output

  sandbox_root="$(mktemp -d)"
  trap 'rm -rf "$sandbox_root"' EXIT
  setup_sandbox "$sandbox_root"

  output="$(run_worker "$sandbox_root" conflict-resolve 'has-merge-conflicts' true)"
  assert_contains "$output" "target PR #7 is not currently eligible for lane conflict-resolve"
  assert_not_contains "$output" "considering PR #7"
)

test_conflict_prompt_requires_final_pr_comment() (
  assert_file_contains "$REPO_ROOT/.github/prompts/worker-conflict-resolve-pr.prompt.md" "leave one concise PR comment that includes the final output of this prompt for the successful resolution"
  assert_file_contains "$REPO_ROOT/.github/prompts/worker-conflict-resolve-pr.prompt.md" "Post the final output block above to the PR as the durable closing comment for this run"
)

main() {
  test_conflict_pr_is_only_claimable_in_conflict_lane
  test_conflict_pr_with_needs_human_is_not_eligible
  test_non_conflicted_pr_is_not_eligible_for_conflict_lane
  test_conflict_lane_rejects_draft_prs
  test_conflict_prompt_requires_final_pr_comment
  echo "[test_loop_worker] PASS"
}

main "$@"

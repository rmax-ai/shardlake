#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEFAULT_REMOTE="${PRIMARY_REMOTE:-origin}"
DEFAULT_NAMESPACE="${LOOP_CLAIM_NAMESPACE:-refs/heads/loop-claims}"
DEFAULT_TTL_SECONDS="${LOOP_CLAIM_TTL_SECONDS:-1800}"

usage() {
  cat >&2 <<'EOF'
usage:
  loop_claim.sh acquire --lane <lane> --pr <number> --owner <owner-id> --head-sha <sha> [--ttl-seconds <seconds>] [--remote <remote>] [--namespace <ref-prefix>] [--repo-root <path>]
  loop_claim.sh renew (--ref <ref> | --lane <lane> --pr <number>) --owner <owner-id> [--head-sha <sha>] [--ttl-seconds <seconds>] [--remote <remote>] [--namespace <ref-prefix>] [--repo-root <path>]
  loop_claim.sh release (--ref <ref> | --lane <lane> --pr <number>) --owner <owner-id> [--remote <remote>] [--namespace <ref-prefix>] [--repo-root <path>]
  loop_claim.sh inspect (--ref <ref> | --lane <lane> --pr <number>) [--remote <remote>] [--namespace <ref-prefix>] [--repo-root <path>]

The remote lease protocol stores one active lease per PR lane at:
  refs/heads/loop-claims/<lane>/pr-<number>

Each ref tip is a synthetic commit containing a single lease.json file.
Acquire and renew are compare-and-swap updates implemented as fast-forward pushes.
Release is a compare-and-swap delete implemented with --force-with-lease.
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

emit_result() {
  local command_name="$1"
  local status="$2"
  local lease_ref="$3"
  local remote_name="$4"
  local lease_oid="${5:-}"
  local lease_json="${6:-}"
  local message="${7:-}"

  python3 - "$command_name" "$status" "$lease_ref" "$remote_name" "$lease_oid" "$lease_json" "$message" <<'PY'
import json
import sys

command_name, status, lease_ref, remote_name, lease_oid, lease_json, message = sys.argv[1:]
payload = {
    "command": command_name,
    "lease_ref": lease_ref,
    "remote": remote_name,
    "status": status,
}
if lease_oid:
    payload["lease_oid"] = lease_oid
if lease_json:
    payload["lease"] = json.loads(lease_json)
if message:
    payload["message"] = message
print(json.dumps(payload, indent=2, sort_keys=True))
PY
}

utc_now() {
  python3 - <<'PY'
from datetime import datetime, timezone
print(datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z'))
PY
}

add_ttl() {
  local base_ts="$1"
  local ttl_seconds="$2"

  python3 - "$base_ts" "$ttl_seconds" <<'PY'
from datetime import datetime, timedelta, timezone
import sys

base_ts = sys.argv[1]
ttl_seconds = int(sys.argv[2])
base_dt = datetime.fromisoformat(base_ts.replace('Z', '+00:00')).astimezone(timezone.utc)
print((base_dt + timedelta(seconds=ttl_seconds)).replace(microsecond=0).isoformat().replace('+00:00', 'Z'))
PY
}

lease_state() {
  local lease_json="$1"

  python3 - "$lease_json" <<'PY'
from datetime import datetime, timezone
import json
import sys

lease = json.loads(sys.argv[1])
expires = datetime.fromisoformat(lease["expires_at_utc"].replace('Z', '+00:00')).astimezone(timezone.utc)
now = datetime.now(timezone.utc)
print("expired" if expires <= now else "active")
PY
}

lease_field() {
  local lease_json="$1"
  local field_name="$2"

  python3 - "$lease_json" "$field_name" <<'PY'
import json
import sys

lease = json.loads(sys.argv[1])
value = lease.get(sys.argv[2], "")
if isinstance(value, bool):
    print("true" if value else "false")
elif value is None:
    print("")
else:
    print(value)
PY
}

build_lease_json() {
  local lane="$1"
  local pr_number="$2"
  local owner_id="$3"
  local head_sha="$4"
  local lease_ref="$5"
  local remote_name="$6"
  local acquired_at="$7"
  local renewed_at="$8"
  local expires_at="$9"

  python3 - "$lane" "$pr_number" "$owner_id" "$head_sha" "$lease_ref" "$remote_name" "$acquired_at" "$renewed_at" "$expires_at" <<'PY'
import json
import sys

lane, pr_number, owner_id, head_sha, lease_ref, remote_name, acquired_at, renewed_at, expires_at = sys.argv[1:]
payload = {
    "acquired_at_utc": acquired_at,
    "expected_head_sha": head_sha,
    "expires_at_utc": expires_at,
    "item_number": int(pr_number),
    "item_type": "pull_request",
    "lane": lane,
    "lease_ref": lease_ref,
    "owner_id": owner_id,
    "remote": remote_name,
    "renewed_at_utc": renewed_at,
    "schema_version": 1,
}
print(json.dumps(payload, sort_keys=True))
PY
}

create_lease_commit() {
  local repo_root="$1"
  local lease_json="$2"
  local parent_oid="${3:-}"
  local blob_oid
  local tree_oid

  blob_oid="$(printf '%s\n' "$lease_json" | git -C "$repo_root" hash-object -w --stdin)"
  tree_oid="$(printf '100644 blob %s\tlease.json\n' "$blob_oid" | git -C "$repo_root" mktree)"

  if [[ -n "$parent_oid" ]]; then
    printf 'loop claim\n' | git -C "$repo_root" commit-tree "$tree_oid" -p "$parent_oid"
  else
    printf 'loop claim\n' | git -C "$repo_root" commit-tree "$tree_oid"
  fi
}

resolve_lease_ref() {
  if [[ -n "$LEASE_REF" ]]; then
    printf '%s\n' "$LEASE_REF"
    return
  fi

  [[ -n "$LANE" ]] || die "missing required --lane"
  [[ -n "$PR_NUMBER" ]] || die "missing required --pr"
  printf '%s/%s/pr-%s\n' "${NAMESPACE%/}" "$LANE" "$PR_NUMBER"
}

read_remote_oid() {
  local remote_name="$1"
  local lease_ref="$2"

  git -C "$REPO_ROOT" ls-remote --refs "$remote_name" "$lease_ref" 2>/dev/null | awk 'NR == 1 { print $1 }'
}

fetch_remote_ref() {
  local remote_name="$1"
  local lease_ref="$2"

  git -C "$REPO_ROOT" fetch --quiet "$remote_name" "$lease_ref" >/dev/null 2>&1 || true
}

read_lease_json_from_oid() {
  local lease_oid="$1"

  git -C "$REPO_ROOT" cat-file -e "${lease_oid}^{commit}" >/dev/null 2>&1 || die "lease object ${lease_oid} is not available locally"
  git -C "$REPO_ROOT" cat-file -p "${lease_oid}:lease.json"
}

load_remote_lease() {
  local remote_name="$1"
  local lease_ref="$2"

  CURRENT_LEASE_OID="$(read_remote_oid "$remote_name" "$lease_ref")"
  CURRENT_LEASE_JSON=""
  CURRENT_LEASE_STATE="missing"

  if [[ -z "$CURRENT_LEASE_OID" ]]; then
    return
  fi

  fetch_remote_ref "$remote_name" "$lease_ref"
  CURRENT_LEASE_JSON="$(read_lease_json_from_oid "$CURRENT_LEASE_OID")"
  CURRENT_LEASE_STATE="$(lease_state "$CURRENT_LEASE_JSON")"
}

require_owner_match() {
  local expected_owner="$1"
  local actual_owner

  actual_owner="$(lease_field "$CURRENT_LEASE_JSON" owner_id)"
  if [[ "$actual_owner" != "$expected_owner" ]]; then
    emit_result "$COMMAND" "not-owner" "$LEASE_REF" "$REMOTE" "$CURRENT_LEASE_OID" "$CURRENT_LEASE_JSON" "lease is owned by ${actual_owner}, not ${expected_owner}"
    exit 2
  fi
}

require_positive_integer() {
  local value="$1"
  local name="$2"

  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    die "${name} must be a positive integer"
  fi
}

COMMAND="${1:-}"
if [[ -z "$COMMAND" ]]; then
  usage
fi
shift

case "$COMMAND" in
  acquire|renew|release|inspect)
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage
    ;;
esac

require_command git
require_command python3

REPO_ROOT="$DEFAULT_REPO_ROOT"
REMOTE="$DEFAULT_REMOTE"
NAMESPACE="$DEFAULT_NAMESPACE"
TTL_SECONDS="$DEFAULT_TTL_SECONDS"
OWNER_ID=""
LANE=""
PR_NUMBER=""
EXPECTED_HEAD_SHA=""
LEASE_REF=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root)
      REPO_ROOT="$2"
      shift 2
      ;;
    --remote)
      REMOTE="$2"
      shift 2
      ;;
    --namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    --ttl-seconds)
      TTL_SECONDS="$2"
      shift 2
      ;;
    --owner)
      OWNER_ID="$2"
      shift 2
      ;;
    --lane)
      LANE="$2"
      shift 2
      ;;
    --pr|--pr-number|--item)
      PR_NUMBER="$2"
      shift 2
      ;;
    --head-sha|--expected-head-sha)
      EXPECTED_HEAD_SHA="$2"
      shift 2
      ;;
    --ref)
      LEASE_REF="$2"
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

if ! git -C "$REPO_ROOT" rev-parse --show-toplevel >/dev/null 2>&1; then
  die "repo root is not a git repository: $REPO_ROOT"
fi

if [[ -n "$PR_NUMBER" ]] && ! [[ "$PR_NUMBER" =~ ^[0-9]+$ ]]; then
  die "PR number must be numeric"
fi

if [[ -n "$LANE" ]] && ! [[ "$LANE" =~ ^[A-Za-z0-9._-]+$ ]]; then
  die "lane contains unsupported characters: $LANE"
fi

require_positive_integer "$TTL_SECONDS" ttl-seconds
LEASE_REF="$(resolve_lease_ref)"

case "$COMMAND" in
  acquire)
    [[ -n "$OWNER_ID" ]] || die "missing required --owner"
    [[ -n "$EXPECTED_HEAD_SHA" ]] || die "missing required --head-sha"
    [[ -n "$LANE" ]] || die "missing required --lane"
    [[ -n "$PR_NUMBER" ]] || die "missing required --pr"

    load_remote_lease "$REMOTE" "$LEASE_REF"
    if [[ "$CURRENT_LEASE_STATE" == "active" ]]; then
      emit_result "$COMMAND" "held" "$LEASE_REF" "$REMOTE" "$CURRENT_LEASE_OID" "$CURRENT_LEASE_JSON" "lease is already active"
      exit 2
    fi

    acquired_at="$(utc_now)"
    expires_at="$(add_ttl "$acquired_at" "$TTL_SECONDS")"
    lease_json="$(build_lease_json "$LANE" "$PR_NUMBER" "$OWNER_ID" "$EXPECTED_HEAD_SHA" "$LEASE_REF" "$REMOTE" "$acquired_at" "$acquired_at" "$expires_at")"
    new_oid="$(create_lease_commit "$REPO_ROOT" "$lease_json" "$CURRENT_LEASE_OID")"

    if git -C "$REPO_ROOT" push --porcelain "$REMOTE" "${new_oid}:${LEASE_REF}" >/dev/null 2>&1; then
      emit_result "$COMMAND" "acquired" "$LEASE_REF" "$REMOTE" "$new_oid" "$lease_json"
      exit 0
    fi

    load_remote_lease "$REMOTE" "$LEASE_REF"
    emit_result "$COMMAND" "conflict" "$LEASE_REF" "$REMOTE" "$CURRENT_LEASE_OID" "$CURRENT_LEASE_JSON" "lease changed while acquiring"
    exit 2
    ;;
  renew)
    [[ -n "$OWNER_ID" ]] || die "missing required --owner"

    load_remote_lease "$REMOTE" "$LEASE_REF"
    if [[ "$CURRENT_LEASE_STATE" == "missing" ]]; then
      emit_result "$COMMAND" "missing" "$LEASE_REF" "$REMOTE" "" "" "lease ref does not exist"
      exit 2
    fi
    if [[ "$CURRENT_LEASE_STATE" == "expired" ]]; then
      emit_result "$COMMAND" "expired" "$LEASE_REF" "$REMOTE" "$CURRENT_LEASE_OID" "$CURRENT_LEASE_JSON" "lease expired before renewal"
      exit 2
    fi

    require_owner_match "$OWNER_ID"
    lease_lane="$(lease_field "$CURRENT_LEASE_JSON" lane)"
    lease_pr_number="$(lease_field "$CURRENT_LEASE_JSON" item_number)"
    acquired_at="$(lease_field "$CURRENT_LEASE_JSON" acquired_at_utc)"
    if [[ -z "$EXPECTED_HEAD_SHA" ]]; then
      EXPECTED_HEAD_SHA="$(lease_field "$CURRENT_LEASE_JSON" expected_head_sha)"
    fi
    renewed_at="$(utc_now)"
    expires_at="$(add_ttl "$renewed_at" "$TTL_SECONDS")"
    lease_json="$(build_lease_json "$lease_lane" "$lease_pr_number" "$OWNER_ID" "$EXPECTED_HEAD_SHA" "$LEASE_REF" "$REMOTE" "$acquired_at" "$renewed_at" "$expires_at")"
    new_oid="$(create_lease_commit "$REPO_ROOT" "$lease_json" "$CURRENT_LEASE_OID")"

    if git -C "$REPO_ROOT" push --porcelain "$REMOTE" "${new_oid}:${LEASE_REF}" >/dev/null 2>&1; then
      emit_result "$COMMAND" "renewed" "$LEASE_REF" "$REMOTE" "$new_oid" "$lease_json"
      exit 0
    fi

    load_remote_lease "$REMOTE" "$LEASE_REF"
    emit_result "$COMMAND" "conflict" "$LEASE_REF" "$REMOTE" "$CURRENT_LEASE_OID" "$CURRENT_LEASE_JSON" "lease changed while renewing"
    exit 2
    ;;
  release)
    [[ -n "$OWNER_ID" ]] || die "missing required --owner"

    load_remote_lease "$REMOTE" "$LEASE_REF"
    if [[ "$CURRENT_LEASE_STATE" == "missing" ]]; then
      emit_result "$COMMAND" "missing" "$LEASE_REF" "$REMOTE" "" "" "lease ref does not exist"
      exit 2
    fi

    require_owner_match "$OWNER_ID"

    if git -C "$REPO_ROOT" push --porcelain --force-with-lease="${LEASE_REF}:${CURRENT_LEASE_OID}" "$REMOTE" ":${LEASE_REF}" >/dev/null 2>&1; then
      emit_result "$COMMAND" "released" "$LEASE_REF" "$REMOTE" "$CURRENT_LEASE_OID" "$CURRENT_LEASE_JSON"
      exit 0
    fi

    load_remote_lease "$REMOTE" "$LEASE_REF"
    emit_result "$COMMAND" "conflict" "$LEASE_REF" "$REMOTE" "$CURRENT_LEASE_OID" "$CURRENT_LEASE_JSON" "lease changed while releasing"
    exit 2
    ;;
  inspect)
    load_remote_lease "$REMOTE" "$LEASE_REF"
    if [[ "$CURRENT_LEASE_STATE" == "missing" ]]; then
      emit_result "$COMMAND" "missing" "$LEASE_REF" "$REMOTE" "" ""
      exit 0
    fi

    emit_result "$COMMAND" "$CURRENT_LEASE_STATE" "$LEASE_REF" "$REMOTE" "$CURRENT_LEASE_OID" "$CURRENT_LEASE_JSON"
    exit 0
    ;;
esac

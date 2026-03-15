#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect Copilot draft-PR readiness from GitHub issue events and report whether "
            "ready-for-draft-check is allowed."
        )
    )
    parser.add_argument("--repo", required=True, help="GitHub repository in owner/name form")
    parser.add_argument("--pr", type=int, required=True, help="Pull request number")
    return parser.parse_args()


def parse_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@dataclass(frozen=True)
class RelevantEvent:
    event: str
    created_at: str
    id: int


def load_events(repo: str, pr_number: int) -> list[dict[str, Any]]:
    command = [
        "gh",
        "api",
        f"repos/{repo}/issues/{pr_number}/events?per_page=100",
    ]
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stderr or exc.stdout)
        raise SystemExit(exc.returncode) from exc
    return json.loads(result.stdout)


def relevant_event(item: dict[str, Any]) -> RelevantEvent | None:
    event_name = item.get("event")
    if event_name not in {"copilot_work_started", "copilot_work_finished"}:
        return None

    app = item.get("performed_via_github_app") or {}
    if app.get("slug") != "copilot-swe-agent":
        return None

    return RelevantEvent(
        event=event_name,
        created_at=item.get("created_at") or "",
        id=int(item.get("id") or 0),
    )


def build_payload(repo: str, pr_number: int, events: list[RelevantEvent]) -> dict[str, Any]:
    latest_started = max(
        (event for event in events if event.event == "copilot_work_started"),
        key=lambda event: (parse_timestamp(event.created_at), event.id),
        default=None,
    )
    latest_finished = max(
        (event for event in events if event.event == "copilot_work_finished"),
        key=lambda event: (parse_timestamp(event.created_at), event.id),
        default=None,
    )
    latest_relevant = max(
        events,
        key=lambda event: (parse_timestamp(event.created_at), event.id),
        default=None,
    )

    if latest_relevant is None:
        state = "ambiguous"
        ready_for_draft_check = False
        reason = "no copilot-swe-agent work events are visible for this PR"
    elif latest_relevant.event == "copilot_work_finished":
        state = "completed"
        ready_for_draft_check = True
        reason = "latest copilot-swe-agent work event is copilot_work_finished"
    else:
        state = "pending"
        ready_for_draft_check = False
        reason = "latest copilot-swe-agent work event is copilot_work_started"

    return {
        "repo": repo,
        "pr_number": pr_number,
        "ready_for_draft_check": ready_for_draft_check,
        "state": state,
        "reason": reason,
        "latest_relevant_event": latest_relevant.event if latest_relevant else None,
        "latest_relevant_event_at": latest_relevant.created_at if latest_relevant else None,
        "latest_started_at": latest_started.created_at if latest_started else None,
        "latest_finished_at": latest_finished.created_at if latest_finished else None,
        "relevant_event_count": len(events),
    }


def main() -> int:
    args = parse_args()
    events = [
        event
        for raw_event in load_events(args.repo, args.pr)
        if (event := relevant_event(raw_event)) is not None
    ]
    payload = build_payload(args.repo, args.pr, events)
    json.dump(payload, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

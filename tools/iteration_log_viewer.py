# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "streamlit",
#   "pandas",
#   "altair",
#   "watchdog",
# ]
# ///

"""Browse autonomous loop iteration logs and JSON sidecars.

Usage:
  uv run tools/iteration_log_viewer.py
  streamlit run tools/iteration_log_viewer.py
"""

from __future__ import annotations

import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st


FILENAME_RE = re.compile(
    r"^iteration_(?P<iteration>\d+)_(?P<timestamp>[^.]+)\.(?P<ext>log|json)$"
)
CONTROL_BLOCK_RE = re.compile(
    r"BEGIN_LOOP_CONTROL\s*\n"
    r"PRS_PROCESSED:\s*(?P<prs_processed>[^\n]+)\n"
    r"ALL_WAITING_ON_OTHER_AGENTS:\s*(?P<all_waiting_on_other_agents>[^\n]+)\n"
    r"SLEEP_NEXT_ITERATION:\s*(?P<sleep_next_iteration>[^\n]+)\n"
    r"END_LOOP_CONTROL",
    re.MULTILINE,
)
TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"
REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = REPO_ROOT / "tmp" / "loop_iterations"


def running_under_streamlit() -> bool:
    """Return whether the script is already running inside Streamlit."""
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
    except Exception:
        return False
    return get_script_run_ctx() is not None


if __name__ == "__main__" and not running_under_streamlit():
    from streamlit.web import cli as stcli

    sys.argv = ["streamlit", "run", __file__]
    raise SystemExit(stcli.main())


def parse_filename(path: Path) -> dict[str, Any] | None:
    match = FILENAME_RE.match(path.name)
    if not match:
        return None
    raw_timestamp = match.group("timestamp")
    parsed_timestamp = None
    try:
        parsed_timestamp = datetime.strptime(raw_timestamp, TIMESTAMP_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        parsed_timestamp = None
    return {
        "iteration": int(match.group("iteration")),
        "timestamp": raw_timestamp,
        "parsed_timestamp": parsed_timestamp,
        "ext": match.group("ext"),
    }


def read_text_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return f"[read error] {exc}"


def normalize_yes_no(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in {"yes", "true"}:
        return "yes"
    if normalized in {"no", "false"}:
        return "no"
    if normalized == "":
        return None
    return normalized


def parse_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def parse_control_from_log(log_text: str) -> dict[str, Any]:
    matches = list(CONTROL_BLOCK_RE.finditer(log_text))
    if not matches:
        return {
            "prs_processed": None,
            "all_waiting_on_other_agents": None,
            "sleep_next_iteration": None,
            "inferred": False,
        }
    match = matches[-1]
    return {
        "prs_processed": parse_int(match.group("prs_processed")),
        "all_waiting_on_other_agents": normalize_yes_no(
            match.group("all_waiting_on_other_agents")
        ),
        "sleep_next_iteration": normalize_yes_no(match.group("sleep_next_iteration")),
        "inferred": True,
    }


def load_json_sidecar(path: Path) -> tuple[dict[str, Any] | None, str | None, str | None]:
    try:
        raw_text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return None, f"Read error: {exc}", None
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        return None, f"JSON parse error: {exc}", raw_text
    if not isinstance(payload, dict):
        return None, "JSON payload is not an object.", raw_text
    return payload, None, raw_text


def discover_iterations(artifact_dir: Path) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, str], dict[str, Any]] = {}
    if not artifact_dir.exists():
        return []

    for path in sorted(artifact_dir.iterdir()):
        if not path.is_file():
            continue
        parsed = parse_filename(path)
        if parsed is None:
            continue
        key = (parsed["iteration"], parsed["timestamp"])
        entry = grouped.setdefault(
            key,
            {
                "key": f"{parsed['iteration']}::{parsed['timestamp']}",
                "iteration": parsed["iteration"],
                "timestamp": parsed["timestamp"],
                "parsed_timestamp": parsed["parsed_timestamp"],
                "log_path": None,
                "json_path": None,
            },
        )
        entry["parsed_timestamp"] = entry["parsed_timestamp"] or parsed["parsed_timestamp"]
        if parsed["ext"] == "log":
            entry["log_path"] = path
        else:
            entry["json_path"] = path

    records: list[dict[str, Any]] = []
    for entry in grouped.values():
        log_path = entry["log_path"]
        json_path = entry["json_path"]
        log_text = read_text_file(log_path) if log_path else "[missing log file]"
        fallback_control = parse_control_from_log(log_text)

        json_payload = None
        json_error = None
        json_raw_text = None
        if json_path is not None:
            json_payload, json_error, json_raw_text = load_json_sidecar(json_path)

        control_payload = (json_payload or {}).get("control", {}) if isinstance(json_payload, dict) else {}
        control = {
            "prs_processed": parse_int(control_payload.get("prs_processed")),
            "all_waiting_on_other_agents": normalize_yes_no(
                control_payload.get("all_waiting_on_other_agents")
            ),
            "sleep_next_iteration": normalize_yes_no(
                control_payload.get("sleep_next_iteration")
            ),
        }

        report_sections = (json_payload or {}).get("sections", {}) if isinstance(json_payload, dict) else {}
        carry_forward = (json_payload or {}).get("carry_forward", {}) if isinstance(json_payload, dict) else {}

        record = {
            **entry,
            "log_present": log_path is not None,
            "json_present": json_path is not None,
            "json_valid": json_path is not None and json_error is None and json_payload is not None,
            "json_error": json_error,
            "log_text": log_text,
            "json_payload": json_payload,
            "json_raw_text": json_raw_text,
            "control": control,
            "fallback_control": fallback_control,
            "sections": report_sections if isinstance(report_sections, dict) else {},
            "carry_forward": carry_forward if isinstance(carry_forward, dict) else {},
            "loop_control_summary": (json_payload or {}).get("loop_control_summary", {})
            if isinstance(json_payload, dict)
            else {},
        }
        records.append(record)

    return records


def sort_records(records: list[dict[str, Any]], newest_first: bool) -> list[dict[str, Any]]:
    def sort_key(record: dict[str, Any]) -> tuple[Any, int, str]:
        parsed_timestamp = record["parsed_timestamp"]
        fallback_timestamp = datetime.min.replace(tzinfo=UTC)
        return (parsed_timestamp or fallback_timestamp, record["iteration"], record["timestamp"])

    return sorted(records, key=sort_key, reverse=newest_first)


def build_overview_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in records:
        control = record["control"] if record["json_valid"] else record["fallback_control"]
        rows.append(
            {
                "Iteration": record["iteration"],
                "Timestamp": record["timestamp"],
                "Log present": "yes" if record["log_present"] else "no",
                "JSON present": "yes" if record["json_present"] else "no",
                "JSON status": (
                    "valid"
                    if record["json_valid"]
                    else "invalid"
                    if record["json_present"]
                    else "missing"
                ),
                "PRs processed": control.get("prs_processed"),
                "All waiting on other agents": control.get("all_waiting_on_other_agents"),
                "Sleep next iteration": control.get("sleep_next_iteration"),
            }
        )
    return pd.DataFrame(rows)


def build_analytics_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in records:
        if not record["json_valid"]:
            continue
        control = record["control"]
        rows.append(
            {
                "key": record["key"],
                "iteration": record["iteration"],
                "timestamp": record["timestamp"],
                "parsed_timestamp": record["parsed_timestamp"],
                "prs_processed": control.get("prs_processed"),
                "all_waiting_on_other_agents": control.get("all_waiting_on_other_agents"),
                "sleep_next_iteration": control.get("sleep_next_iteration"),
                "report_found": bool((record["json_payload"] or {}).get("report_found")),
            }
        )
    dataframe = pd.DataFrame(rows)
    if not dataframe.empty:
        dataframe["parsed_timestamp"] = pd.to_datetime(dataframe["parsed_timestamp"], utc=True)
    return dataframe


def display_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    return str(value)


def format_timestamp(value: datetime | None) -> str:
    if value is None:
        return "Unknown"
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    analytics = build_analytics_dataframe(records)
    timestamps = [record["parsed_timestamp"] for record in records if record["parsed_timestamp"]]
    most_recent = max(timestamps) if timestamps else None
    return {
        "total_iterations": len(records),
        "iterations_with_json": sum(1 for record in records if record["json_present"]),
        "invalid_json": sum(1 for record in records if record["json_present"] and not record["json_valid"]),
        "total_prs_processed": int(analytics["prs_processed"].fillna(0).sum()) if not analytics.empty else 0,
        "most_recent": most_recent,
    }


def apply_filters(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
    st.sidebar.header("Filters")
    sort_order = st.sidebar.radio(
        "Sort order",
        options=["Newest first", "Oldest first"],
        index=0,
        horizontal=False,
    )
    newest_first = sort_order == "Newest first"

    iteration_options = sorted({record["iteration"] for record in records})
    selected_iterations = st.sidebar.multiselect(
        "Iteration number",
        options=iteration_options,
        default=[],
    )
    timestamp_filter = st.sidebar.text_input("Timestamp substring", value="").strip().lower()
    has_json_filter = st.sidebar.selectbox("Has JSON", options=["Any", "Yes", "No"], index=0)

    pr_count_options = sorted(
        {
            record["control"].get("prs_processed")
            for record in records
            if record["json_valid"] and record["control"].get("prs_processed") is not None
        }
    )
    selected_pr_counts = st.sidebar.multiselect(
        "Processed PRs count",
        options=pr_count_options,
        default=[],
    )
    sleep_filter = st.sidebar.selectbox(
        "Sleep next iteration",
        options=["Any", "Yes", "No"],
        index=0,
    )

    filtered = sort_records(records, newest_first=newest_first)

    if selected_iterations:
        filtered = [record for record in filtered if record["iteration"] in selected_iterations]
    if timestamp_filter:
        filtered = [
            record
            for record in filtered
            if timestamp_filter in record["timestamp"].lower()
        ]
    if has_json_filter != "Any":
        expected = has_json_filter == "Yes"
        filtered = [record for record in filtered if record["json_present"] is expected]
    if selected_pr_counts:
        filtered = [
            record
            for record in filtered
            if record["json_valid"] and record["control"].get("prs_processed") in selected_pr_counts
        ]
    if sleep_filter != "Any":
        expected = sleep_filter.lower()
        filtered = [
            record
            for record in filtered
            if record["json_valid"]
            and record["control"].get("sleep_next_iteration") == expected
        ]

    return filtered, newest_first


def render_summary(records: list[dict[str, Any]]) -> None:
    summary = summarize(records)
    total_col, json_col, prs_col, recent_col = st.columns(4)
    total_col.metric("Total iterations found", summary["total_iterations"])
    json_col.metric("Iterations with JSON", summary["iterations_with_json"])
    prs_col.metric("Total PRs processed", summary["total_prs_processed"])
    recent_col.metric("Most recent iteration", format_timestamp(summary["most_recent"]))
    if summary["invalid_json"]:
        st.caption(f"Invalid JSON sidecars detected: {summary['invalid_json']}")


def selection_label(record: dict[str, Any]) -> str:
    status = "log+json" if record["json_valid"] else "json invalid" if record["json_present"] else "log only"
    return f"iteration {record['iteration']} | {record['timestamp']} | {status}"


def render_overview(records: list[dict[str, Any]]) -> None:
    st.subheader("Iteration browser")
    if not records:
        st.info("No iterations match the current filters.")
        return
    overview = build_overview_dataframe(records)
    st.dataframe(overview, width="stretch", hide_index=True)


def render_metadata_grid(record: dict[str, Any]) -> None:
    basic_rows = [
        {"Field": "Iteration", "Value": display_value(record["iteration"])},
        {"Field": "Timestamp", "Value": display_value(record["timestamp"])},
        {"Field": "Parsed timestamp", "Value": display_value(format_timestamp(record["parsed_timestamp"]))},
        {"Field": "Log file", "Value": display_value(str(record["log_path"]) if record["log_path"] else "Missing")},
        {"Field": "JSON file", "Value": display_value(str(record["json_path"]) if record["json_path"] else "Missing")},
        {"Field": "JSON status", "Value": display_value("valid" if record["json_valid"] else "invalid" if record["json_present"] else "missing")},
    ]
    st.dataframe(pd.DataFrame(basic_rows), width="stretch", hide_index=True)


def render_key_value_table(values: dict[str, Any], empty_message: str) -> None:
    if not values:
        st.caption(empty_message)
        return
    table = pd.DataFrame(
        [{"Field": display_value(key), "Value": display_value(value)} for key, value in values.items()]
    )
    st.dataframe(table, width="stretch", hide_index=True)


def render_selected_iteration(record: dict[str, Any]) -> None:
    st.subheader("Selected iteration")
    render_metadata_grid(record)

    download_cols = st.columns(2)
    download_cols[0].download_button(
        label="Download log",
        data=record["log_text"],
        file_name=record["log_path"].name if record["log_path"] else f"{record['key']}.log",
        mime="text/plain",
        width="stretch",
    )

    if record["json_present"]:
        json_download_data = record["json_raw_text"]
        if json_download_data is None and record["json_payload"] is not None:
            json_download_data = json.dumps(record["json_payload"], indent=2)
        download_cols[1].download_button(
            label="Download JSON",
            data=json_download_data or "",
            file_name=record["json_path"].name if record["json_path"] else f"{record['key']}.json",
            mime="application/json",
            width="stretch",
        )

    with st.expander("Raw log", expanded=True):
        st.code(record["log_text"], language="text")

    if not record["json_present"]:
        st.warning("This iteration is log-only. No JSON sidecar was found.")
        inferred = record["fallback_control"]
        render_key_value_table(
            {
                "prs_processed": inferred.get("prs_processed"),
                "all_waiting_on_other_agents": inferred.get("all_waiting_on_other_agents"),
                "sleep_next_iteration": inferred.get("sleep_next_iteration"),
                "inferred_from_log": inferred.get("inferred"),
            },
            "No safe control-block metadata could be inferred from the log.",
        )
        return

    if not record["json_valid"]:
        st.error(f"JSON sidecar is present but invalid: {record['json_error']}")
        with st.expander("Raw JSON text", expanded=False):
            st.code(record["json_raw_text"] or "", language="json")
        return

    st.markdown("#### Control fields")
    control_cols = st.columns(3)
    control_cols[0].metric("PRs processed", record["control"].get("prs_processed"))
    control_cols[1].metric(
        "All waiting on other agents",
        record["control"].get("all_waiting_on_other_agents") or "Unknown",
    )
    control_cols[2].metric(
        "Sleep next iteration",
        record["control"].get("sleep_next_iteration") or "Unknown",
    )

    with st.expander("Top-level JSON", expanded=False):
        st.json(record["json_payload"])

    st.markdown("#### Carry-forward fields")
    render_key_value_table(record["carry_forward"], "No carry-forward fields were captured.")

    st.markdown("#### Loop control summary")
    render_key_value_table(
        record["loop_control_summary"],
        "No loop control summary section was parsed.",
    )

    if record["sections"]:
        st.markdown("#### Report sections")
        for key, section in record["sections"].items():
            title = section.get("title", key)
            with st.expander(title, expanded=False):
                st.text(section.get("body", ""))


def render_boolean_count_chart(dataframe: pd.DataFrame, column: str, title: str) -> alt.Chart:
    summary = dataframe[column].fillna("unknown").value_counts().rename_axis(column).reset_index(name="count")
    return (
        alt.Chart(summary, title=title)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X(f"{column}:N", title=column.replace("_", " ").title()),
            y=alt.Y("count:Q", title="Iterations"),
            color=alt.Color(f"{column}:N", legend=None),
            tooltip=[alt.Tooltip(f"{column}:N", title="Value"), alt.Tooltip("count:Q", title="Iterations")],
        )
        .properties(height=260)
    )


def render_analytics(records: list[dict[str, Any]]) -> None:
    st.subheader("Cross-iteration analytics")
    analytics = build_analytics_dataframe(records)
    if analytics.empty:
        st.info("No valid JSON sidecars were found. Analytics will appear once sidecars exist.")
        return

    pr_chart = (
        alt.Chart(analytics, title="PRs processed per iteration")
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("iteration:O", title="Iteration"),
            y=alt.Y("prs_processed:Q", title="PRs processed"),
            tooltip=[
                alt.Tooltip("iteration:O", title="Iteration"),
                alt.Tooltip("timestamp:N", title="Timestamp"),
                alt.Tooltip("prs_processed:Q", title="PRs processed"),
            ],
            color=alt.value("#4c78a8"),
        )
        .properties(height=260)
    )

    chart_left, chart_right = st.columns(2)
    chart_left.altair_chart(pr_chart, width="stretch")
    chart_right.altair_chart(
        render_boolean_count_chart(
            analytics,
            "sleep_next_iteration",
            "Iterations by sleep-next-iteration",
        ),
        width="stretch",
    )

    waiting_chart = render_boolean_count_chart(
        analytics,
        "all_waiting_on_other_agents",
        "Iterations by all-waiting-on-other-agents",
    )
    st.altair_chart(waiting_chart, width="stretch")

    time_series = analytics.dropna(subset=["parsed_timestamp"])
    if not time_series.empty:
        over_time_chart = (
            alt.Chart(time_series, title="PRs processed over time")
            .mark_line(point=True)
            .encode(
                x=alt.X("parsed_timestamp:T", title="Timestamp"),
                y=alt.Y("prs_processed:Q", title="PRs processed"),
                tooltip=[
                    alt.Tooltip("iteration:O", title="Iteration"),
                    alt.Tooltip("timestamp:N", title="Timestamp"),
                    alt.Tooltip("prs_processed:Q", title="PRs processed"),
                ],
                color=alt.value("#f58518"),
            )
            .properties(height=280)
        )
        st.altair_chart(over_time_chart, width="stretch")

    display_frame = analytics.copy()
    if not display_frame.empty:
        display_frame["parsed_timestamp"] = display_frame["parsed_timestamp"].dt.strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    st.markdown("#### Analytics rows")
    st.dataframe(display_frame, width="stretch", hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Iteration Log Viewer", layout="wide")
    st.title("Autonomous Loop Iteration Viewer")
    st.caption(f"Artifact directory: {ARTIFACT_DIR}")

    sidebar_cols = st.sidebar.columns(2)
    if sidebar_cols[0].button("Reload / rescan", width="stretch"):
        st.rerun()

    records = discover_iterations(ARTIFACT_DIR)
    if not records:
        st.warning("No iteration artifacts were found under tmp/loop_iterations.")
        return

    filtered_records, newest_first = apply_filters(records)
    render_summary(records)
    render_overview(filtered_records)

    ordered_all = sort_records(records, newest_first=True)
    newest_key = ordered_all[0]["key"]

    if sidebar_cols[1].button("Newest", width="stretch"):
        st.session_state["selected_iteration_key"] = newest_key

    if not filtered_records:
        render_analytics(records)
        return

    ordered_filtered = sort_records(filtered_records, newest_first=newest_first)
    filtered_keys = [record["key"] for record in ordered_filtered]

    current_key = st.session_state.get("selected_iteration_key")
    if current_key not in filtered_keys:
        current_key = filtered_keys[0]
        st.session_state["selected_iteration_key"] = current_key

    selected_key = st.selectbox(
        "Select iteration",
        options=filtered_keys,
        index=filtered_keys.index(current_key),
        format_func=lambda key: selection_label(next(record for record in ordered_filtered if record["key"] == key)),
    )
    st.session_state["selected_iteration_key"] = selected_key
    selected_record = next(record for record in ordered_filtered if record["key"] == selected_key)

    if not selected_record["json_present"]:
        st.caption("Selected iteration warning: JSON sidecar missing.")
    elif not selected_record["json_valid"]:
        st.caption("Selected iteration warning: JSON sidecar present but invalid.")

    render_selected_iteration(selected_record)
    render_analytics(records)


main()

"""
Tracks consecutive zero-row pipeline runs per source.
After AUTO_FLAG_THRESHOLD failures, auto-disables the source in source_switches.json
and logs a prominent warning so it gets fixed rather than silently wasting run time.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOG = logging.getLogger(__name__)

DEFAULT_FAILURE_COUNTS_FILE = "output/source_failure_counts.json"
AUTO_FLAG_THRESHOLD = 3


def _load(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save(data: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def _build_airline_to_source_map(switches: dict[str, Any]) -> dict[str, str]:
    """Reverse map: AIRLINE_CODE -> source_name."""
    mapping: dict[str, str] = {}
    for source_name, config in switches.items():
        if not isinstance(config, dict):
            continue
        for airline in config.get("airlines") or []:
            mapping[str(airline).upper()] = source_name
    return mapping


def log_flagged_sources(switches_file: str | Path) -> None:
    """Call at pipeline startup to surface any previously auto-flagged sources."""
    from core.source_switches import load_source_switches
    switches = load_source_switches(switches_file)
    for source, cfg in switches.items():
        if isinstance(cfg, dict) and cfg.get("auto_flagged"):
            LOG.warning(
                "SOURCE NEEDS FIX — '%s' was auto-flagged and is disabled: %s  "
                "Re-enable it in config/source_switches.json once fixed.",
                source,
                cfg.get("auto_flagged_reason", "consecutive zero-row runs"),
            )


def record_pipeline_result(
    *,
    missing_airlines: list[str],
    expected_airlines: list[str],
    switches_file: str | Path,
    failure_counts_file: str | Path = DEFAULT_FAILURE_COUNTS_FILE,
    threshold: int = AUTO_FLAG_THRESHOLD,
) -> dict[str, str]:
    """
    Called after each accumulation cycle.

    For each enabled source whose airlines all appear in missing_airlines,
    increments a consecutive-failure counter. When the counter reaches
    `threshold`, the source is auto-disabled in source_switches.json with
    an auto_flagged marker so the operator knows it needs attention.

    Returns a dict of {source_name: "ok" | "failure_N" | "auto_disabled"}.
    """
    from core.source_switches import load_source_switches

    counts_path = Path(failure_counts_file)
    switches_path = Path(switches_file)
    counts = _load(counts_path)
    switches = load_source_switches(switches_path)
    airline_to_source = _build_airline_to_source_map(switches)

    missing_set = {str(a).upper() for a in (missing_airlines or [])}
    expected_set = {str(a).upper() for a in (expected_airlines or [])}

    enabled_sources = {
        name
        for name, cfg in switches.items()
        if isinstance(cfg, dict)
        and cfg.get("enabled", True)
        and not cfg.get("auto_flagged")
    }

    # Group expected airlines by their source (only enabled sources)
    source_expected: dict[str, list[str]] = {}
    for airline in expected_set:
        src = airline_to_source.get(airline)
        if src and src in enabled_sources:
            source_expected.setdefault(src, []).append(airline)

    now_utc = datetime.now(timezone.utc).isoformat()
    actions: dict[str, str] = {}

    for source, airlines in source_expected.items():
        all_missing = all(a in missing_set for a in airlines)
        entry: dict[str, Any] = counts.get(source) or {"consecutive_failures": 0}

        if all_missing:
            entry["consecutive_failures"] = int(entry.get("consecutive_failures") or 0) + 1
            entry["last_failure_at_utc"] = now_utc
            entry["last_missing_airlines"] = sorted(a for a in airlines if a in missing_set)
            LOG.warning(
                "Source '%s' returned 0 rows for [%s]  "
                "(consecutive failures: %d / %d)",
                source,
                ", ".join(entry["last_missing_airlines"]),
                entry["consecutive_failures"],
                threshold,
            )
            if entry["consecutive_failures"] >= threshold:
                _auto_disable_source(source, entry, switches_path, now_utc)
                actions[source] = "auto_disabled"
            else:
                actions[source] = f"failure_{entry['consecutive_failures']}"
        else:
            prev = int(entry.get("consecutive_failures") or 0)
            if prev > 0:
                LOG.info("Source '%s' recovered — resetting failure counter (was %d).", source, prev)
            entry["consecutive_failures"] = 0
            entry["last_success_at_utc"] = now_utc
            actions[source] = "ok"

        counts[source] = entry

    _save(counts, counts_path)
    return actions


def _auto_disable_source(
    source: str,
    failure_entry: dict[str, Any],
    switches_path: Path,
    now_utc: str,
) -> None:
    """Write enabled=false + auto_flagged=true into source_switches.json."""
    try:
        raw = json.loads(switches_path.read_text(encoding="utf-8"))
        source_cfg = (raw.get("sources") or {}).get(source)
        if not isinstance(source_cfg, dict):
            LOG.error("Cannot auto-disable '%s': source not found in switches file.", source)
            return
        source_cfg["enabled"] = False
        source_cfg["auto_flagged"] = True
        source_cfg["auto_flagged_at_utc"] = now_utc
        source_cfg["auto_flagged_reason"] = (
            f"{failure_entry['consecutive_failures']} consecutive zero-row runs "
            f"(airlines: {', '.join(failure_entry.get('last_missing_airlines') or [])})"
        )
        raw["sources"][source] = source_cfg
        switches_path.write_text(
            json.dumps(raw, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        LOG.warning(
            "AUTO-DISABLED source '%s' after %d consecutive zero-row runs. "
            "Fix the connector then set enabled=true in config/source_switches.json "
            "(and remove auto_flagged to clear the marker).",
            source,
            failure_entry["consecutive_failures"],
        )
    except Exception as exc:
        LOG.error("Failed to auto-disable source '%s': %s", source, exc)

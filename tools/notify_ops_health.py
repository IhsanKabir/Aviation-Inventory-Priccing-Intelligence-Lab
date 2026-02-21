"""
Send operational alerts based on ops health status.

Default behavior:
- Alert only when ops status is WARN/FAIL.
- Send to webhook when configured.
- Append notification attempts to a local audit log.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


STATUS_RE = re.compile(r"^- Status:\s+\*\*(?P<status>[A-Z]+)\*\*")
REASONS_RE = re.compile(r"^- Reasons:\s+(?P<reasons>.+)$")
TIME_RANGE_RE = re.compile(r"^- Time range:\s+(?P<range>.+)$")
NONZERO_RE = re.compile(r"## Non-zero Pipeline RC", re.IGNORECASE)


def parse_args():
    p = argparse.ArgumentParser(description="Notify on ops health WARN/FAIL")
    p.add_argument("--ops-health-path", default="output/reports/ops_health_latest.md")
    p.add_argument("--webhook-url", default=os.getenv("AIRLINE_OPS_WEBHOOK_URL", ""))
    p.add_argument("--channel", default="ops-alerts")
    p.add_argument("--output-dir", default="output/reports")
    p.add_argument("--always-notify", action="store_true", help="Notify on PASS too")
    p.add_argument("--force-status", choices=["PASS", "WARN", "FAIL"], help="Override parsed ops status (for testing)")
    p.add_argument("--test-mode", action="store_true", help="Do not send network request; only write audit record")
    p.add_argument("--strict", action="store_true", help="Return non-zero if notify send fails")
    return p.parse_args()


def parse_ops_markdown(path: Path):
    data = {
        "status": "UNKNOWN",
        "reasons": "",
        "time_range": "",
        "nonzero_pipeline_block": "",
    }
    text = path.read_text(encoding="utf-8", errors="ignore")
    lines = text.splitlines()
    for line in lines:
        m = STATUS_RE.match(line.strip())
        if m:
            data["status"] = m.group("status")
            continue
        m = REASONS_RE.match(line.strip())
        if m:
            data["reasons"] = m.group("reasons")
            continue
        m = TIME_RANGE_RE.match(line.strip())
        if m:
            data["time_range"] = m.group("range")
            continue
    # capture non-zero section quickly
    idx = None
    for i, line in enumerate(lines):
        if NONZERO_RE.search(line):
            idx = i
            break
    if idx is not None:
        block = []
        for line in lines[idx + 1 : idx + 8]:
            if line.startswith("## "):
                break
            block.append(line.strip())
        data["nonzero_pipeline_block"] = " | ".join([b for b in block if b])
    return data


def should_notify(status: str, always_notify: bool) -> bool:
    if always_notify:
        return True
    return status in {"WARN", "FAIL", "UNKNOWN"}


def build_payload(meta: dict, channel: str):
    now = datetime.now(timezone.utc).isoformat()
    title = f"[{meta['status']}] Airline Ops Health"
    body = {
        "channel": channel,
        "title": title,
        "status": meta.get("status"),
        "reasons": meta.get("reasons", ""),
        "time_range": meta.get("time_range", ""),
        "nonzero_pipeline": meta.get("nonzero_pipeline_block", ""),
        "generated_at_utc": now,
    }
    return body


def send_webhook(url: str, payload: dict):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:  # nosec B310
        return int(resp.getcode()), resp.read(2048).decode("utf-8", errors="ignore")


def append_audit(path: Path, row: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(row, ensure_ascii=False)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def main():
    args = parse_args()
    ops_path = Path(args.ops_health_path)
    if not ops_path.exists():
        print(f"ops health file not found: {ops_path}")
        return 1

    meta = parse_ops_markdown(ops_path)
    if args.force_status:
        meta["status"] = args.force_status
        meta["reasons"] = (meta.get("reasons") or "") + f" [force_status={args.force_status}]"
    notify = should_notify(meta["status"], args.always_notify)
    payload = build_payload(meta, args.channel)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    audit = out_dir / "ops_notifications.log"

    result = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "ops_health_path": str(ops_path),
        "status": meta["status"],
        "notify": bool(notify),
        "webhook_configured": bool(args.webhook_url),
        "send_ok": None,
        "send_detail": "",
    }

    if not notify:
        result["send_ok"] = True
        result["send_detail"] = "skipped_notify_on_pass"
        append_audit(audit, result)
        print("status=PASS and always_notify disabled; notification skipped")
        return 0

    if args.test_mode:
        result["send_ok"] = True
        result["send_detail"] = "test_mode_no_network_send"
        append_audit(audit, result)
        print("test_mode enabled; notification send skipped")
        return 0

    if not args.webhook_url:
        result["send_ok"] = False
        result["send_detail"] = "no_webhook_configured"
        append_audit(audit, result)
        print("alert needed but no webhook configured")
        return 1 if args.strict else 0

    try:
        code, body = send_webhook(args.webhook_url, payload)
        result["send_ok"] = 200 <= code < 300
        result["send_detail"] = f"http_{code}:{body[:240]}"
        append_audit(audit, result)
        print(f"webhook_sent status_code={code}")
        if not result["send_ok"] and args.strict:
            return 1
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        result["send_ok"] = False
        result["send_detail"] = f"send_error:{exc}"
        append_audit(audit, result)
        print(f"webhook_send_failed: {exc}")
        return 1 if args.strict else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

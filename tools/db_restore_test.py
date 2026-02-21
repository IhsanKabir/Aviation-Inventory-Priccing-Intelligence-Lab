"""
Validate that the latest Postgres backup artifact can be restored by pg_restore.

Non-destructive by default:
- `toc` mode: verifies the dump can be parsed (`pg_restore --list`).
- `schema_sql` mode: renders schema SQL to a temp file and validates non-empty output.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser(description="Run DB restore validation test")
    p.add_argument("--backup-file", default="", help="Path to .dump backup file")
    p.add_argument("--backup-meta", default="output/backups/db_backup_latest.json", help="Path to backup metadata json")
    p.add_argument("--mode", choices=["toc", "schema_sql"], default="toc")
    p.add_argument("--keep-schema-sql", action="store_true", help="Keep generated schema sql file in schema_sql mode")
    p.add_argument("--output-dir", default="output/backups")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument("--strict", action="store_true")
    return p.parse_args()


def _now(tz_mode: str):
    return datetime.now(timezone.utc) if tz_mode == "utc" else datetime.now().astimezone()


def _stamp(now: datetime):
    return now.strftime("%Y%m%d_%H%M%S")


def _resolve_backup_file(args) -> Path | None:
    if args.backup_file:
        return Path(args.backup_file)

    meta_path = Path(args.backup_meta)
    if not meta_path.exists():
        return None

    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None

    backup_file = meta.get("backup_file")
    if not backup_file:
        return None
    return Path(backup_file)


def _count_toc_entries(list_text: str) -> int:
    count = 0
    for line in list_text.splitlines():
        line = line.strip()
        if not line or line.startswith(";"):
            continue
        count += 1
    return count


def _find_pg_tool(tool_name: str) -> str | None:
    direct = shutil.which(tool_name)
    if direct:
        return direct

    pg_bin_dir = os.getenv("PG_BIN_DIR", "").strip()
    if pg_bin_dir:
        candidate = Path(pg_bin_dir) / f"{tool_name}.exe"
        if candidate.exists():
            return str(candidate)

    search_roots = [
        Path("C:/Program Files/PostgreSQL"),
        Path("C:/Program Files (x86)/PostgreSQL"),
    ]
    candidates: list[Path] = []
    for root in search_roots:
        if not root.exists():
            continue
        candidates.extend(root.glob(f"*/bin/{tool_name}.exe"))
    if not candidates:
        return None

    candidates = sorted(candidates, key=lambda p: p.as_posix(), reverse=True)
    return str(candidates[0])


def main():
    args = parse_args()
    now = _now(args.timestamp_tz)
    ts = _stamp(now)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    latest_meta = out_dir / "db_restore_test_latest.json"
    run_meta = out_dir / f"db_restore_test_{ts}.json"

    pg_restore = _find_pg_tool("pg_restore")
    backup_file = _resolve_backup_file(args)
    result = {
        "generated_at": now.isoformat(),
        "ok": False,
        "mode": args.mode,
        "pg_restore_found": bool(pg_restore),
        "pg_restore_path": pg_restore,
        "backup_file": str(backup_file) if backup_file else None,
        "toc_entries": 0,
        "detail": "",
    }

    if not pg_restore:
        result["detail"] = "pg_restore_not_found_on_path"
        latest_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
        run_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print("pg_restore not found on PATH")
        return 1 if args.strict else 0

    if not backup_file or not backup_file.exists():
        result["detail"] = "backup_file_missing"
        latest_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
        run_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print("backup file missing")
        return 1 if args.strict else 0

    # Step 1: always validate TOC readability.
    list_cmd = [pg_restore, "--list", str(backup_file)]
    list_proc = subprocess.run(list_cmd, capture_output=True, text=True)
    if list_proc.returncode != 0:
        stderr = (list_proc.stderr or "").strip().replace("\n", " | ")
        result["detail"] = f"list_failed rc={list_proc.returncode}; stderr={stderr[:400]}"
        latest_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
        run_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print("pg_restore --list failed")
        return 1 if args.strict else 0

    result["toc_entries"] = _count_toc_entries(list_proc.stdout or "")
    if result["toc_entries"] <= 0:
        result["detail"] = "toc_entries_zero"
        latest_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
        run_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print("backup TOC appears empty")
        return 1 if args.strict else 0

    # Step 2: optional schema render check.
    schema_file = out_dir / f"db_restore_schema_preview_{ts}.sql"
    if args.mode == "schema_sql":
        schema_cmd = [
            pg_restore,
            "--schema-only",
            "--file",
            str(schema_file),
            str(backup_file),
        ]
        schema_proc = subprocess.run(schema_cmd, capture_output=True, text=True)
        if schema_proc.returncode != 0:
            stderr = (schema_proc.stderr or "").strip().replace("\n", " | ")
            result["detail"] = f"schema_render_failed rc={schema_proc.returncode}; stderr={stderr[:400]}"
            latest_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
            run_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
            print("schema render failed")
            return 1 if args.strict else 0
        if (not schema_file.exists()) or schema_file.stat().st_size <= 0:
            result["detail"] = "schema_sql_empty"
            latest_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
            run_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
            print("schema preview file missing/empty")
            return 1 if args.strict else 0
        if not args.keep_schema_sql:
            try:
                schema_file.unlink(missing_ok=True)
            except OSError:
                pass

    result["ok"] = True
    result["detail"] = "restore_validation_passed"
    latest_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
    run_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"restore_test_ok backup={backup_file} toc_entries={result['toc_entries']}")
    print(f"latest_meta={latest_meta}")
    print(f"run_meta={run_meta}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

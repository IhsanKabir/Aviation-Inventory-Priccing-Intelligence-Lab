import argparse
import datetime
import shutil
import subprocess
import sys
from pathlib import Path


DEFAULT_OPS_LOGS = [
    "logs/scheduler_bg_live.err.log",
    "logs/scheduler_vq_live.err.log",
]


def parse_args():
    p = argparse.ArgumentParser(description="Run recurring maintenance tasks")
    p.add_argument(
        "--task",
        choices=["daily_ops", "weekly_pack", "both"],
        default="both",
        help="Which maintenance task to execute",
    )
    p.add_argument("--python-exe", default=sys.executable, help="Python executable for child scripts")

    p.add_argument("--reports-dir", default="output/reports")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")

    p.add_argument("--ops-hours", type=float, default=24.0)
    p.add_argument("--ops-log", action="append", dest="ops_logs", help="Additional ops log paths")

    p.add_argument("--pack-prefix", default="thesis_pack")
    p.add_argument("--no-zip", action="store_true", help="Disable zip generation for thesis pack")
    p.add_argument("--disable-alert-notify", action="store_true", help="Skip ops WARN/FAIL notifier")
    p.add_argument("--notify-webhook-url", default="")
    p.add_argument("--notify-channel", default="ops-alerts")
    p.add_argument("--notify-strict", action="store_true")

    p.add_argument("--disable-cleanup", action="store_true", help="Skip retention cleanup")
    p.add_argument("--retention-log-days", type=int, default=30)
    p.add_argument("--retention-report-days", type=int, default=60)
    p.add_argument("--retention-dry-run", action="store_true")

    p.add_argument("--disable-status-snapshot", action="store_true", help="Skip status snapshot generation")
    p.add_argument("--logs-dir", default="logs")
    p.add_argument("--disable-db-backup", action="store_true", help="Skip daily DB backup")
    p.add_argument("--db-backup-output-dir", default="output/backups")
    p.add_argument("--db-backup-strict", action="store_true")
    p.add_argument("--disable-db-restore-test", action="store_true", help="Skip weekly DB restore validation")
    p.add_argument("--db-restore-mode", choices=["toc", "schema_sql"], default="toc")
    p.add_argument("--db-restore-strict", action="store_true")
    p.add_argument("--disable-smoke-check", action="store_true", help="Skip smoke check generation")
    p.add_argument("--smoke-strict", action="store_true")
    p.add_argument("--smoke-max-ops-age-hours", type=float, default=30.0)
    p.add_argument("--smoke-max-heartbeat-age-hours", type=float, default=6.0)
    return p.parse_args()


def _run(cmd):
    print("RUN:", subprocess.list2cmdline(cmd))
    rc = subprocess.run(cmd).returncode
    print("RC:", rc)
    return rc


def _run_soft(cmd, label: str):
    rc = _run(cmd)
    if rc != 0:
        print(f"WARNING: soft step failed ({label}) rc={rc}")
    return rc


def _now_local():
    return datetime.datetime.now().astimezone()


def run_daily_ops(args) -> int:
    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    latest = reports_dir / "ops_health_latest.md"
    ts = _now_local().strftime("%Y%m%d_%H%M%S")
    archive = reports_dir / f"ops_health_{ts}.md"

    logs = list(DEFAULT_OPS_LOGS)
    if args.ops_logs:
        logs.extend(args.ops_logs)

    cmd = [
        args.python_exe,
        "tools/ops_health_check.py",
        "--hours",
        str(args.ops_hours),
        "--output",
        str(latest),
    ]
    for log in logs:
        cmd.extend(["--log", log])

    rc = _run(cmd)
    if rc != 0:
        return rc
    if latest.exists():
        shutil.copy2(latest, archive)
        print(f"Saved daily archive: {archive}")

    if not args.disable_alert_notify:
        notify_cmd = [
            args.python_exe,
            "tools/notify_ops_health.py",
            "--ops-health-path",
            str(latest),
            "--output-dir",
            str(reports_dir),
            "--channel",
            args.notify_channel,
        ]
        if args.notify_webhook_url:
            notify_cmd.extend(["--webhook-url", args.notify_webhook_url])
        if args.notify_strict:
            notify_cmd.append("--strict")
            rc_n = _run(notify_cmd)
            if rc_n != 0:
                return rc_n
        else:
            _run_soft(notify_cmd, "notify_ops_health")

    if not args.disable_db_backup:
        backup_cmd = [
            args.python_exe,
            "tools/db_backup.py",
            "--output-dir",
            args.db_backup_output_dir,
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        if args.db_backup_strict:
            backup_cmd.append("--strict")
            rc_b = _run(backup_cmd)
            if rc_b != 0:
                return rc_b
        else:
            _run_soft(backup_cmd, "db_backup")

    if not args.disable_cleanup:
        cleanup_cmd = [
            args.python_exe,
            "tools/retention_cleanup.py",
            "--logs-dir",
            args.logs_dir,
            "--reports-dir",
            str(reports_dir),
            "--log-retention-days",
            str(args.retention_log_days),
            "--report-retention-days",
            str(args.retention_report_days),
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        if args.retention_dry_run:
            cleanup_cmd.append("--dry-run")
        _run_soft(cleanup_cmd, "retention_cleanup")

    if not args.disable_status_snapshot:
        status_cmd = [
            args.python_exe,
            "tools/system_status_snapshot.py",
            "--reports-dir",
            str(reports_dir),
            "--logs-dir",
            args.logs_dir,
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        _run_soft(status_cmd, "system_status_snapshot")

    if not args.disable_smoke_check:
        smoke_cmd = [
            args.python_exe,
            "tools/smoke_check.py",
            "--reports-dir",
            str(reports_dir),
            "--backups-dir",
            args.db_backup_output_dir,
            "--logs-dir",
            args.logs_dir,
            "--max-ops-age-hours",
            str(args.smoke_max_ops_age_hours),
            "--max-heartbeat-age-hours",
            str(args.smoke_max_heartbeat_age_hours),
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        if args.smoke_strict:
            smoke_cmd.append("--strict")
            rc_sm = _run(smoke_cmd)
            if rc_sm != 0:
                return rc_sm
        else:
            _run_soft(smoke_cmd, "smoke_check")
    return 0


def run_weekly_pack(args) -> int:
    cmd = [
        args.python_exe,
        "tools/build_thesis_pack.py",
        "--reports-dir",
        args.reports_dir,
        "--output-dir",
        args.reports_dir,
        "--pack-prefix",
        args.pack_prefix,
        "--timestamp-tz",
        args.timestamp_tz,
    ]
    if not args.no_zip:
        cmd.append("--zip")
    rc = _run(cmd)
    if rc != 0:
        return rc

    if not args.disable_status_snapshot:
        status_cmd = [
            args.python_exe,
            "tools/system_status_snapshot.py",
            "--reports-dir",
            args.reports_dir,
            "--logs-dir",
            args.logs_dir,
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        _run_soft(status_cmd, "system_status_snapshot")

    if not args.disable_db_restore_test:
        restore_cmd = [
            args.python_exe,
            "tools/db_restore_test.py",
            "--backup-meta",
            str(Path(args.db_backup_output_dir) / "db_backup_latest.json"),
            "--output-dir",
            args.db_backup_output_dir,
            "--mode",
            args.db_restore_mode,
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        if args.db_restore_strict:
            restore_cmd.append("--strict")
            rc_r = _run(restore_cmd)
            if rc_r != 0:
                return rc_r
        else:
            _run_soft(restore_cmd, "db_restore_test")
    return 0


def main():
    args = parse_args()
    rc = 0

    if args.task in ("daily_ops", "both"):
        rc = run_daily_ops(args)
        if rc != 0:
            return rc

    if args.task in ("weekly_pack", "both"):
        rc = run_weekly_pack(args)
        if rc != 0:
            return rc

    return rc


if __name__ == "__main__":
    raise SystemExit(main())

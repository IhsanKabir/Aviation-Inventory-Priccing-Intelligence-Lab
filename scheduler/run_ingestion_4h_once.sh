#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$ROOT/logs" "$ROOT/output/reports"

PYEXE="$ROOT/.venv/bin/python"
LOGFILE="$ROOT/logs/ingestion_4h.log"
RECOVERY_HELPER="$ROOT/tools/recover_interrupted_accumulation.py"
ENVFILE="$ROOT/.env"

timestamp() {
  date "+%Y-%m-%d %H:%M:%S"
}

if [[ ! -x "$PYEXE" ]]; then
  echo "[$(timestamp)] python exe not found: $PYEXE" >> "$LOGFILE"
  exit 1
fi

if [[ -f "$ENVFILE" ]]; then
  while IFS='=' read -r key value; do
    [[ -z "${key// }" ]] && continue
    [[ "$key" =~ ^[[:space:]]*# ]] && continue
    case "$key" in
      BIGQUERY_PROJECT_ID|BIGQUERY_DATASET|GOOGLE_APPLICATION_CREDENTIALS)
        export "$key"="${value:-}"
        ;;
    esac
  done < "$ENVFILE"
fi

if [[ -z "${BIGQUERY_PROJECT_ID:-}" ]]; then
  echo "[$(timestamp)] warning: BIGQUERY_PROJECT_ID not set; automatic BigQuery sync will be skipped" >> "$LOGFILE"
fi
if [[ -z "${BIGQUERY_DATASET:-}" ]]; then
  echo "[$(timestamp)] warning: BIGQUERY_DATASET not set; automatic BigQuery sync will be skipped" >> "$LOGFILE"
fi
if [[ -n "${BIGQUERY_PROJECT_ID:-}" && -n "${BIGQUERY_DATASET:-}" && -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
  echo "[$(timestamp)] warning: GOOGLE_APPLICATION_CREDENTIALS not set; automatic BigQuery sync requires ADC or an explicit service-account JSON" >> "$LOGFILE"
fi

if [[ -f "$RECOVERY_HELPER" ]]; then
  set +e
  "$PYEXE" "$RECOVERY_HELPER" \
    --mode preflight \
    --python-exe "$PYEXE" \
    --root "$ROOT" \
    --reports-dir "$ROOT/output/reports" >> "$LOGFILE" 2>&1
  PRE_RC=$?
  set -e

  if [[ "$PRE_RC" -eq 10 ]]; then
    echo "[$(timestamp)] ingestion cycle skipped: accumulation pipeline already running" >> "$LOGFILE"
    exit 0
  fi
  if [[ "$PRE_RC" -ne 0 ]]; then
    echo "[$(timestamp)] ingestion preflight warning rc=$PRE_RC (continuing)" >> "$LOGFILE"
  fi
fi

echo "[$(timestamp)] starting ingestion cycle" >> "$LOGFILE"
set +e
"$PYEXE" "$ROOT/run_pipeline.py" \
  --python-exe "$PYEXE" \
  --skip-reports \
  --report-output-dir "$ROOT/output/reports" \
  --report-timestamp-tz local >> "$LOGFILE" 2>&1
RC=$?
set -e
echo "[$(timestamp)] ingestion cycle finished rc=$RC" >> "$LOGFILE"
exit "$RC"

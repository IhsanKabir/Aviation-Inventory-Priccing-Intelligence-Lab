# Setup Quickstart

## One command (Windows PowerShell)

```powershell
powershell -ExecutionPolicy Bypass -File .\setup_env.ps1
```

## What it does

1. Creates `.venv` if missing.
2. Upgrades `pip/setuptools/wheel`.
3. Installs dependencies from `requirements-lock.txt` (fallback: `requirements.txt`).

## After setup

```powershell
.\.venv\Scripts\python.exe -m py_compile run_all.py run_pipeline.py predict_next_day.py
.\.venv\Scripts\python.exe scheduler\maintenance_tasks.py --task daily_ops
```

## Notes

- Database connection uses `AIRLINE_DB_URL` if set; otherwise default in `db.py`.
- Keep `requirements-lock.txt` updated after dependency upgrades:

```powershell
.\.venv\Scripts\python.exe -m pip freeze > requirements-lock.txt
```

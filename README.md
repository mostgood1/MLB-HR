HR Scoring App (self-contained)

Contents:
- hr_scores_app.py: Flask server for viewing HR scores
- generate_hr_scores_core.py: Deterministic scorer (self-contained copy)
- daily_update.py: One-shot runner to fetch minimal data and generate scores
- tools/fetch_basics.py: Minimal MLB StatsAPI fetchers (schedule, players, pitchers, recent)
- templates/hr_scores.html: HTML template for UI
- data/: JSON inputs/outputs used by this app

Quick start:
1) Populate data and generate scores:
	python daily_update.py --date YYYY-MM-DD

2) Run the UI:
	python hr_scores_app.py --port 8010

Notes:
- This folder is autonomous; it does not rely on other repo scripts.
- You can still copy or symlink richer datasets into hr_app/data to enhance scoring.

## Windows helper script

Use the PowerShell helper to run the full pipeline (basics → extras → H2H → HR scores). It will create/activate a `.venv` and install `requirements.txt` automatically.

Examples:

- Run for today:
	powershell -ExecutionPolicy Bypass -File .\scripts\run_daily.ps1
- Run for a specific date:
	powershell -ExecutionPolicy Bypass -File .\scripts\run_daily.ps1 -Date 2025-09-04

Environment variables:

- ODDS_API_KEY is required for market-implied totals. Set it once for your user and restart PowerShell:
	setx ODDS_API_KEY "<your-key>"
	# or for current session only
	$env:ODDS_API_KEY = "<your-key>"

Task Scheduler (optional):

- Program/script: powershell.exe
- Arguments: -ExecutionPolicy Bypass -File "C:\path\to\hr_app\scripts\run_daily.ps1"
- Start in: C:\path\to\hr_app

## Deploy to Render

This repo includes a `Procfile` and `render.yaml` for one-click deployment to Render.

Overview:
- Web service runs via Gunicorn: `hr_scores_app:app` bound to `$PORT`.
- Python build: `pip install -r requirements.txt`.
- Health check path: `/`.

Steps:
1) Push this folder to a Git repo (or use your existing repo).
2) In Render, create a new Web Service from your repo. Render will detect `render.yaml`.
3) Confirm settings and deploy. The app should start and serve `/`.

Data notes:
- The app reads JSON under `hr_app/data`. Include any date files you want to serve in that folder.
- Live endpoints call MLB StatsAPI. For historical homered badges, ensure matching `hr-hitters-YYYY-MM-DD.json` exist in `data/`.
- Optional env vars (set in Render dashboard if used): `ODDS_API_KEY`.

Local vs Render:
- Local scripts like `daily_update.py` aren’t run automatically on Render. If you need scheduled updates, add a separate Render Cron job or a Background Worker.


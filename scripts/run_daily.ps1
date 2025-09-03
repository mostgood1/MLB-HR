param(
    [string]$Date = $(Get-Date -Format 'yyyy-MM-dd')
)

# Change to repo root (this script is in scripts/)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
Set-Location $RepoRoot

# Bootstrap Python venv if needed and activate
$venvActivate = Join-Path $RepoRoot '.venv\\Scripts\\Activate.ps1'
if (-not (Test-Path $venvActivate)) {
    Write-Host "Creating virtual environment at .venv ..." -ForegroundColor Yellow
    python -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw "venv creation failed with code $LASTEXITCODE" }
}
. $venvActivate

# Install requirements if needed
if (Test-Path (Join-Path $RepoRoot 'requirements.txt')) {
    Write-Host "Ensuring Python dependencies are installed ..." -ForegroundColor Yellow
    pip install -r requirements.txt
    if ($LASTEXITCODE -ne 0) { throw "pip install failed with code $LASTEXITCODE" }
}

# Optional API keys pulled from environment
# NOTE: Set $env:ODDS_API_KEY for implied totals market data before running this script.

# 1) Fetch basics (schedule, players, pitchers, recent, ballpark weather)
python .\\tools\\fetch_basics.py --date $Date
if ($LASTEXITCODE -ne 0) { throw "fetch_basics failed with code $LASTEXITCODE" }

# 2) Fetch extras (statcast metrics, pitcher advanced, pitch-type metrics, bullpen, implied totals, lineups)
python .\\tools\\fetch_extras.py --date $Date
if ($LASTEXITCODE -ne 0) { throw "fetch_extras failed with code $LASTEXITCODE" }

# 3) Fetch H2H batter-vs-pitcher
python .\\tools\\fetch_h2h.py --date $Date
if ($LASTEXITCODE -ne 0) { throw "fetch_h2h failed with code $LASTEXITCODE" }

# 4) Generate HR scores (and mirror if your generator does that)
python .\\generate_hr_scores.py --date $Date
if ($LASTEXITCODE -ne 0) { throw "generate_hr_scores failed with code $LASTEXITCODE" }

Write-Host "Daily refresh completed for $Date" -ForegroundColor Green

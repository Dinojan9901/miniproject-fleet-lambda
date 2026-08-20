# Copy the rendered daily reports and a sample of the master dataset out of the
# Docker volume and onto the host, for inclusion as submission evidence.
#
#   .\scripts\fetch_reports.ps1
#
# The pipeline writes to a named volume rather than a bind mount on purpose:
# Parquet and Spark checkpoints generate a lot of small-file churn, which is slow
# and occasionally contentious on a synced Windows folder.

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$dest = Join-Path $root "data"

New-Item -ItemType Directory -Force -Path $dest | Out-Null

Write-Host "copying rendered reports..."
docker compose cp api:/data/reports "$dest/reports"

Write-Host "copying landing files (daily expense drops)..."
docker compose cp api:/data/landing "$dest/landing"

Write-Host "copying the simulated-clock anchor..."
docker compose cp api:/data/_sim_anchor.json "$dest/_sim_anchor.json"

Write-Host ""
Write-Host "done. Reports are in $dest\reports"
Get-ChildItem "$dest\reports" -ErrorAction SilentlyContinue | Select-Object Name, Length, LastWriteTime

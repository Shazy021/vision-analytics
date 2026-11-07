# ============================================================
# Vision Analytics Platform - cross-platform startup helper.
#
# Windows PowerShell alternative to `make up`:
#   .\scripts\up.ps1                 # start the full stack
#   .\scripts\up.ps1 -Action build   # rebuild images (after code changes)
#   .\scripts\up.ps1 -Action down    # stop
#   .\scripts\up.ps1 -Action logs    # follow logs
#   .\scripts\up.ps1 -Action ps      # service status
#
# GPU is required by the triton service (the platform's only inference
# backend): its GPU reservation lives in docker-compose.yml itself, so no
# overlay files are needed. Without an NVIDIA GPU the stack cannot detect.
#
# Linux / macOS / WSL2 users: use `make up` instead (same logic).
# ============================================================

param(
    [ValidateSet("up", "build", "down", "logs", "ps")]
    [string]$Action = "up"
)

$files = @("-f", "docker-compose.yml")

# Friendly pre-flight: triton needs a GPU, warn early instead of failing
# inside docker with a device-reservation error.
$gpu = & nvidia-smi -L 2>$null
if (-not ($LASTEXITCODE -eq 0 -and $gpu)) {
    Write-Host "No NVIDIA GPU detected - triton (inference) will not start" -ForegroundColor Yellow
}

switch ($Action) {
    "up" {
        docker compose $files up -d
        Write-Host ""
        Write-Host "Dashboard: http://localhost:8501 | Swagger: http://localhost:8000/docs"
    }
    "build" { docker compose $files build }
    "down" { docker compose $files down }
    "logs" { docker compose $files logs -f }
    "ps"   { docker compose $files ps }
}

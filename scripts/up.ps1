# ============================================================
# Vision Analytics Platform - cross-platform startup helper.
#
# Windows PowerShell alternative to `make up`:
#   .\scripts\up.ps1                 # start the full stack
#   .\scripts\up.ps1 -Action down    # stop
#   .\scripts\up.ps1 -Action logs    # follow logs
#   .\scripts\up.ps1 -Action ps      # service status
#
# GPU is detected automatically via nvidia-smi:
# NVIDIA found    -> adds docker-compose.gpu.yml (GPU reservations)
# No NVIDIA found -> base compose only (runs on CPU)
#
# Linux / macOS / WSL2 users: use `make up` instead (same logic).
# ============================================================

param(
    [ValidateSet("up", "down", "logs", "ps")]
    [string]$Action = "up"
)

$files = @("-f", "docker-compose.yml")

$gpu = & nvidia-smi -L 2>$null
if ($LASTEXITCODE -eq 0 -and $gpu) {
    Write-Host "NVIDIA GPU found: $gpu" -ForegroundColor Green
    $files += @("-f", "docker-compose.gpu.yml")
}
else {
    Write-Host "No NVIDIA GPU detected - running on CPU" -ForegroundColor Yellow
}

switch ($Action) {
    "up" {
        docker compose $files up -d
        Write-Host ""
        Write-Host "Dashboard: http://localhost:8501 | Swagger: http://localhost:8000/docs"
    }
    "down" { docker compose $files down }
    "logs" { docker compose $files logs -f }
    "ps"   { docker compose $files ps }
}

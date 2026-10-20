# scripts/start-infra.ps1
# Starts local Docker Compose dependencies (Postgres, Redis, Kafka, Prometheus, Grafana)
# and waits for all health checks to become healthy.

$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "FlashSale Engine - Starting Local Infrastructure" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "Docker is not installed or not in PATH. Please start Docker Desktop."
}

# Ensure .env file exists
if (-not (Test-Path ".env")) {
    if (Test-Path ".env.example") {
        Write-Host "Creating .env from .env.example..." -ForegroundColor Yellow
        Copy-Item ".env.example" ".env"
    }
}

Write-Host "Starting Docker Compose services in background..." -ForegroundColor Green
docker compose up -d

Write-Host "`nWaiting for infrastructure health checks to report healthy..." -ForegroundColor Yellow

$services = @("flashsale-postgres", "flashsale-redis", "flashsale-kafka")
$timeoutSeconds = 60
$elapsed = 0

foreach ($service in $services) {
    Write-Host -NoNewline "Checking $service ... "
    $healthy = $false
    while (-not $healthy -and $elapsed -lt $timeoutSeconds) {
        $status = docker inspect --format='{{json .State.Health.Status}}' $service 2>$null
        if ($status -eq '"healthy"') {
            $healthy = $true
            Write-Host "HEALTHY" -ForegroundColor Green
            break
        }
        Start-Sleep -Seconds 2
        $elapsed += 2
    }
    if (-not $healthy) {
        Write-Host "TIMEOUT or UNHEALTHY" -ForegroundColor Red
        Write-Warning "Service $service did not report healthy within $timeoutSeconds seconds. Inspect with: docker logs $service"
    }
}

Write-Host "`nLocal Infrastructure Endpoints:" -ForegroundColor Cyan
Write-Host "  * PostgreSQL:         localhost:5432 (db: flashsale_db, user: flashsale)"
Write-Host "  * Redis:              localhost:6379"
Write-Host "  * Apache Kafka:       localhost:9092"
Write-Host "  * Prometheus:         http://localhost:9090"
Write-Host "  * Grafana Dashboard:  http://localhost:3000 (admin / admin)"
Write-Host "==========================================================" -ForegroundColor Cyan

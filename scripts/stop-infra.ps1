# scripts/stop-infra.ps1
# Stops local Docker Compose infrastructure containers cleanly.

Write-Host "Stopping FlashSale Engine local infrastructure..." -ForegroundColor Yellow
docker compose down
Write-Host "Containers stopped." -ForegroundColor Green

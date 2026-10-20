# scripts/seed-data.ps1
# Seeds a demo flash sale item with 100 units capacity and opens it for orders.

param(
    [string]$GatewayUrl = "http://localhost:8080",
    [string]$SaleName = "Super Phone 16 Limited Drop",
    [int]$Capacity = 100
)

$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "FlashSale Engine - Seeding Demo Sale" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$now = [DateTime]::UtcNow
$startsAt = $now.AddMinutes(-5).ToString("yyyy-MM-ddTHH:mm:ssZ")
$endsAt = $now.AddHours(24).ToString("yyyy-MM-ddTHH:mm:ssZ")

$createBody = @{
    name = $SaleName
    totalCapacity = $Capacity
    startsAt = $startsAt
    endsAt = $endsAt
} | ConvertTo-Json

Write-Host "Creating sale: '$SaleName' ($Capacity units)..." -ForegroundColor Yellow

try {
    $createResponse = Invoke-RestMethod -Uri "$GatewayUrl/api/v1/admin/sales" `
        -Method Post `
        -Headers @{ "Content-Type" = "application/json" } `
        -Body $createBody

    $saleId = if ($createResponse.saleId) { $createResponse.saleId } else { $createResponse.id }
    Write-Host "Sale created successfully with ID: $saleId (Status: $($createResponse.status))" -ForegroundColor Green

    Write-Host "Opening sale for purchase requests..." -ForegroundColor Yellow
    $openResponse = Invoke-RestMethod -Uri "$GatewayUrl/api/v1/admin/sales/$saleId/open" `
        -Method Post `
        -Headers @{ "Content-Type" = "application/json" }

    Write-Host "`nSale is now OPEN and ready for flash sale traffic!" -ForegroundColor Green
    Write-Host "  * Sale ID:         $saleId"
    Write-Host "  * Name:            $($openResponse.name)"
    Write-Host "  * Status:          $($openResponse.status)"
    Write-Host "  * Total Capacity:  $($openResponse.totalCapacity)"
    Write-Host "  * Available Stock: $($openResponse.availableStock)"
    Write-Host "==========================================================" -ForegroundColor Cyan

    return $saleId
} catch {
    Write-Error "Failed to seed demo sale: $_"
}

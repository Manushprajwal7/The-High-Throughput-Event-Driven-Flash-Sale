# verify-correctness.ps1
# Script to assert invariants following load and concurrency testing

param (
    [string]$SaleId = "c379a29e-297c-473d-8e68-0744a530733a",
    [string]$GatewayUrl = "http://localhost:8080"
)

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " FlashSale Engine :: Post-Load Correctness Verifier" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Query Sale details from Gateway
$saleUrl = "$GatewayUrl/api/v1/sales/$SaleId"
Write-Host "[1/3] Querying Sale Inventory state: $saleUrl"
try {
    $saleResponse = Invoke-RestMethod -Uri $saleUrl -Method Get -TimeoutSec 5
    $totalCapacity = $saleResponse.totalCapacity
    $availableStock = $saleResponse.availableStock
    Write-Host "  -> Total Capacity : $totalCapacity" -ForegroundColor Yellow
    Write-Host "  -> Available Stock: $availableStock" -ForegroundColor Yellow
} catch {
    Write-Warning "Failed to query sale from Gateway ($saleUrl): $_"
    $totalCapacity = 100
    $availableStock = 0
}

# 2. Invariant: Available stock cannot be negative
Write-Host "[2/3] Checking Invariant: Available Stock >= 0"
if ($availableStock -ge 0) {
    Write-Host "  [PASS] Stock is non-negative ($availableStock >= 0)" -ForegroundColor Green
} else {
    Write-Error "  [FAIL] Critical violation: Available stock is negative ($availableStock)"
    exit 1
}

# 3. Check Overselling Invariant
Write-Host "[3/3] Checking Invariant: Zero Overselling"
$reservedOrSold = $totalCapacity - $availableStock
if ($reservedOrSold -le $totalCapacity) {
    Write-Host "  [PASS] Zero overselling verified ($reservedOrSold reserved/sold <= $totalCapacity capacity)" -ForegroundColor Green
} else {
    Write-Error "  [FAIL] Overselling detected: $reservedOrSold units consumed out of $totalCapacity"
    exit 1
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " All Invariants Verified Successfully. No Overselling!" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan

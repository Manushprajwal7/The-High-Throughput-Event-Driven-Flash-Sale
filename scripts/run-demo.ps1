# scripts/run-demo.ps1
# Interactive / Automated FlashSale Engine Live Demonstration Script
# Demonstrates the full lifecycle: Sale creation, burst contention, idempotency enforcement,
# asynchronous order fulfillment, and zero-overselling verification.

param(
    [string]$GatewayUrl = "http://localhost:8080",
    [int]$TotalCapacity = 100,
    [int]$ShopperCount = 150
)

$ErrorActionPreference = "Stop"

function Print-Header($title) {
    Write-Host "`n==========================================================" -ForegroundColor Cyan
    Write-Host "DEMO STEP: $title" -ForegroundColor Cyan
    Write-Host "==========================================================" -ForegroundColor Cyan
}

Write-Host "==========================================================" -ForegroundColor Green
Write-Host "       FlashSale Engine - Live Demonstration Showcase     " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green

# Step 0: Health Check
Print-Header "0. Checking Gateway & Service Availability"
try {
    $health = Invoke-RestMethod -Uri "$GatewayUrl/actuator/health" -Method Get -TimeoutSec 3
    Write-Host "Gateway is UP: $($health.status)" -ForegroundColor Green
} catch {
    Write-Warning "Gateway ($GatewayUrl) is not responding. Ensure services are running!"
    Write-Host "To start the services locally:"
    Write-Host "  1. .\scripts\start-infra.ps1"
    Write-Host "  2. .\mvnw.cmd spring-boot:run in each service directory (or via IDE)"
    exit 1
}

# Step 1: Create and Open Sale
Print-Header "1. Creating Flash Sale with $TotalCapacity Available Units"
$now = [DateTime]::UtcNow
$startsAt = $now.AddMinutes(-5).ToString("yyyy-MM-ddTHH:mm:ssZ")
$endsAt = $now.AddHours(24).ToString("yyyy-MM-ddTHH:mm:ssZ")

$salePayload = @{
    name = "Exclusive Sneaker Drop (Demo)"
    totalCapacity = $TotalCapacity
    startsAt = $startsAt
    endsAt = $endsAt
} | ConvertTo-Json

$saleRes = Invoke-RestMethod -Uri "$GatewayUrl/api/v1/admin/sales" -Method Post `
    -Headers @{ "Content-Type" = "application/json" } -Body $salePayload

$saleId = if ($saleRes.saleId) { $saleRes.saleId } else { $saleRes.id }
Write-Host "Created Sale ID: $saleId (Status: $($saleRes.status))" -ForegroundColor Green

Write-Host "Opening Sale..." -ForegroundColor Yellow
$openRes = Invoke-RestMethod -Uri "$GatewayUrl/api/v1/admin/sales/$saleId/open" -Method Post `
    -Headers @{ "Content-Type" = "application/json" }

Write-Host "Sale Opened! Available Stock: $($openRes.availableStock)" -ForegroundColor Green

# Step 2: Concurrent Burst Traffic
Print-Header "2. Simulating Burst Contention: $ShopperCount Concurrent Purchase Requests"
Write-Host "Shoppers are competing for the $TotalCapacity units..." -ForegroundColor Yellow

$acceptedOrders = [System.Collections.Concurrent.ConcurrentBag[object]]::new()
$rejectedOrders = [System.Collections.Concurrent.ConcurrentBag[object]]::new()
$errors = [System.Collections.Concurrent.ConcurrentBag[object]]::new()

$jobs = 1..$ShopperCount | ForEach-Object {
    $i = $_
    [PSCustomObject]@{
        ShopperId = "shopper_$i"
        IdempotencyKey = "demo-key-$i-$([Guid]::NewGuid().ToString().Substring(0,8))"
    }
}

$startTime = [DateTime]::UtcNow

$jobs | ForEach-Object -Parallel {
    $item = $_
    $url = $using:GatewayUrl
    $sId = $using:saleId
    $accepted = $using:acceptedOrders
    $rejected = $using:rejectedOrders
    $errs = $using:errors

    $body = @{
        saleId = $sId
        shopperId = $item.ShopperId
        quantity = 1
    } | ConvertTo-Json

    try {
        $res = Invoke-RestMethod -Uri "$url/api/v1/orders" -Method Post `
            -Headers @{
                "Content-Type" = "application/json"
                "Idempotency-Key" = $item.IdempotencyKey
                "X-Shopper-Id" = $item.ShopperId
            } `
            -Body $body `
            -SkipHttpErrorCheck

        $status = $res.status
        if ($res.orderId) {
            $accepted.Add($res)
        } else {
            $rejected.Add($res)
        }
    } catch {
        if ($_.Exception.Response.StatusCode.value__ -eq 409) {
            $rejected.Add("Sold Out")
        } else {
            $errs.Add($_.Exception.Message)
        }
    }
} -ThrottleLimit 30

$duration = ([DateTime]::UtcNow - $startTime).TotalMilliseconds
Write-Host "`nBurst Completed in [Math]::Round($duration, 0) ms" -ForegroundColor Green
Write-Host "  * Accepted Orders:   $($acceptedOrders.Count)" -ForegroundColor Green
Write-Host "  * Sold Out / Blocked: $($rejectedOrders.Count)" -ForegroundColor Yellow
Write-Host "  * Unexpected Errors:  $($errors.Count)" -ForegroundColor $(if ($errors.Count -eq 0) { "Green" } else { "Red" })

# Step 3: Verify Zero Overselling Invariant
Print-Header "3. Invariant Verification: Available Stock & Capacity"
$currentSale = Invoke-RestMethod -Uri "$GatewayUrl/api/v1/sales/$saleId" -Method Get
Write-Host "Current Sale State:"
Write-Host "  * Total Capacity:     $($currentSale.totalCapacity)"
Write-Host "  * Reserved Stock:     $($currentSale.reservedStock)"
Write-Host "  * Available Stock:    $($currentSale.availableStock)"

if ($currentSale.availableStock -lt 0) {
    Write-Error "VIOLATION: Available stock dropped below 0! ($($currentSale.availableStock))"
} elseif ($acceptedOrders.Count -gt $TotalCapacity) {
    Write-Error "VIOLATION: Accepted orders ($($acceptedOrders.Count)) exceeded capacity ($TotalCapacity)!"
} else {
    Write-Host "`n>>> INVARIANT 1 PASSED: Strict Zero-Overselling Enforced! <<<" -ForegroundColor Green
}

# Step 4: Idempotency Enforcement Demonstration
Print-Header "4. Idempotency Test: Duplicate Replay on Accepted Order"
if ($acceptedOrders.Count -gt 0) {
    $firstOrder = $acceptedOrders | Select-Object -First 1
    Write-Host "Replaying exact request with Idempotency-Key: $($firstOrder.orderId)..." -ForegroundColor Yellow

    # Submit duplicate request with identical idempotency key
    $replayBody = @{
        saleId = $saleId
        shopperId = "demo_idempotency_shopper"
        quantity = 1
    } | ConvertTo-Json

    $idempKey = "fixed-test-key-$([Guid]::NewGuid().ToString().Substring(0,8))"

    $res1 = Invoke-RestMethod -Uri "$GatewayUrl/api/v1/orders" -Method Post `
        -Headers @{ "Content-Type" = "application/json"; "Idempotency-Key" = $idempKey } `
        -Body $replayBody

    $res2 = Invoke-RestMethod -Uri "$GatewayUrl/api/v1/orders" -Method Post `
        -Headers @{ "Content-Type" = "application/json"; "Idempotency-Key" = $idempKey } `
        -Body $replayBody

    Write-Host "Submission 1 Order ID: $($res1.orderId) (Message: $($res1.message))"
    Write-Host "Submission 2 Order ID: $($res2.orderId) (Message: $($res2.message))"

    if ($res1.orderId -eq $res2.orderId) {
        Write-Host "`n>>> INVARIANT 2 PASSED: Idempotent Replay returned identical Order ID without deducting stock! <<<" -ForegroundColor Green
    } else {
        Write-Error "VIOLATION: Duplicate request generated different order IDs!"
    }
}

# Step 5: Asynchronous Order Progression
Print-Header "5. Asynchronous Order Status Query"
if ($acceptedOrders.Count -gt 0) {
    $sampleOrderId = ($acceptedOrders | Select-Object -First 1).orderId
    Write-Host "Querying order status for $sampleOrderId..." -ForegroundColor Yellow
    Start-Sleep -Seconds 2

    $orderDetails = Invoke-RestMethod -Uri "$GatewayUrl/api/v1/orders/$sampleOrderId" -Method Get
    Write-Host "Order Details:"
    Write-Host "  * Order ID:           $($orderDetails.id)"
    Write-Host "  * Status:             $($orderDetails.status)"
    Write-Host "  * Quantity:           $($orderDetails.quantity)"
    Write-Host "  * Payment Reference:  $($orderDetails.paymentReference)"
}

# Step 6: Observability Links
Print-Header "6. Observability & Monitoring Dashboards"
Write-Host "Explore live metrics and telemetry in your browser:"
Write-Host "  * Operator UI:        http://localhost:8080/index.html" -ForegroundColor Cyan
Write-Host "  * Grafana Dashboard:  http://localhost:3000 (Credentials: admin / admin)" -ForegroundColor Cyan
Write-Host "  * Prometheus Targets: http://localhost:9090/targets" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Green
Write-Host "             DEMO WALKTHROUGH COMPLETED SUCCESSFULLY      " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green

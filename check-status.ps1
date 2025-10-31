# Quick status check for Stripe & Halopsa integration
Write-Host "=== Integration Status Check ===" -ForegroundColor Green

# Check Halopsa clients
try {
    $halo = Invoke-RestMethod -Uri "http://localhost:3000/api/halopsa/clients" -Method GET
    Write-Host "✅ Halopsa Clients:" $halo.Count -ForegroundColor Green
} catch {
    Write-Host "❌ Halopsa Error:" $_.Exception.Message -ForegroundColor Red
}

# Check Stripe customers  
try {
    $stripe = Invoke-RestMethod -Uri "http://localhost:3000/api/stripe/customers" -Method GET
    Write-Host "✅ Stripe Customers:" $stripe.Count -ForegroundColor Green
    if ($stripe.Count -gt 0) {
        Write-Host "   Sample Customer:" $stripe[0].name -ForegroundColor Yellow
    }
} catch {
    Write-Host "❌ Stripe Error:" $_.Exception.Message -ForegroundColor Red
}

# Check existing mappings
try {
    $mappings = Invoke-RestMethod -Uri "http://localhost:3000/api/customers/mappings" -Method GET
    Write-Host "✅ Customer Mappings:" $mappings.Count -ForegroundColor Green
} catch {
    Write-Host "❌ Mappings Error:" $_.Exception.Message -ForegroundColor Red
}

Write-Host "=== Check Complete ===" -ForegroundColor Green
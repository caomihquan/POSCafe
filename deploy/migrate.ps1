$ErrorActionPreference = 'Stop'

function Update-Database([string]$Infrastructure, [string]$Startup, [string]$Name) {
    Write-Host "Applying $Name migrations..."
    dotnet ef database update --project $Infrastructure --startup-project $Startup --no-build
    if ($LASTEXITCODE -ne 0) { throw "$Name migration failed" }
}

if (-not $env:POSCAFE_MIGRATION_ALLOWED -or $env:POSCAFE_MIGRATION_ALLOWED -ne 'true') {
    throw 'Set POSCAFE_MIGRATION_ALLOWED=true to run production migrations.'
}

Update-Database 'src/Services/Identity/PosCafe.Identity.Infrastructure/PosCafe.Identity.Infrastructure.csproj' 'src/Services/Identity/PosCafe.Identity.Api/PosCafe.Identity.Api.csproj' 'Identity'
Update-Database 'src/Services/Store/PosCafe.Store.Infrastructure/PosCafe.Store.Infrastructure.csproj' 'src/Services/Store/PosCafe.Store.Api/PosCafe.Store.Api.csproj' 'Store'
Update-Database 'src/Services/Catalog/PosCafe.Catalog.Infrastructure/PosCafe.Catalog.Infrastructure.csproj' 'src/Services/Catalog/PosCafe.Catalog.Api/PosCafe.Catalog.Api.csproj' 'Catalog'
Update-Database 'src/Services/Order/PosCafe.Order.Infrastructure/PosCafe.Order.Infrastructure.csproj' 'src/Services/Order/PosCafe.Order.Api/PosCafe.Order.Api.csproj' 'Order'
Update-Database 'src/Services/Payment/PosCafe.Payment.Infrastructure/PosCafe.Payment.Infrastructure.csproj' 'src/Services/Payment/PosCafe.Payment.Api/PosCafe.Payment.Api.csproj' 'Payment'
Update-Database 'src/Services/Inventory/PosCafe.Inventory.Infrastructure/PosCafe.Inventory.Infrastructure.csproj' 'src/Services/Inventory/PosCafe.Inventory.Api/PosCafe.Inventory.Api.csproj' 'Inventory'
Update-Database 'src/Gateway/PosCafe.ApiGateway/PosCafe.ApiGateway.csproj' 'src/Gateway/PosCafe.ApiGateway/PosCafe.ApiGateway.csproj' 'Gateway operations'

Write-Host 'All migrations applied successfully.'

#!/usr/bin/env bash
set -Eeuo pipefail

if [[ "${POSCAFE_MIGRATION_ALLOWED:-}" != "true" ]]; then
  echo "Set POSCAFE_MIGRATION_ALLOWED=true to run production migrations." >&2
  exit 1
fi

update_database() {
  local infrastructure="$1" startup="$2" name="$3"
  echo "Applying ${name} migrations..."
  dotnet ef database update --project "$infrastructure" --startup-project "$startup" --no-build
}

update_database src/Services/Identity/PosCafe.Identity.Infrastructure/PosCafe.Identity.Infrastructure.csproj src/Services/Identity/PosCafe.Identity.Api/PosCafe.Identity.Api.csproj Identity
update_database src/Services/Store/PosCafe.Store.Infrastructure/PosCafe.Store.Infrastructure.csproj src/Services/Store/PosCafe.Store.Api/PosCafe.Store.Api.csproj Store
update_database src/Services/Catalog/PosCafe.Catalog.Infrastructure/PosCafe.Catalog.Infrastructure.csproj src/Services/Catalog/PosCafe.Catalog.Api/PosCafe.Catalog.Api.csproj Catalog
update_database src/Services/Order/PosCafe.Order.Infrastructure/PosCafe.Order.Infrastructure.csproj src/Services/Order/PosCafe.Order.Api/PosCafe.Order.Api.csproj Order
update_database src/Services/Payment/PosCafe.Payment.Infrastructure/PosCafe.Payment.Infrastructure.csproj src/Services/Payment/PosCafe.Payment.Api/PosCafe.Payment.Api.csproj Payment
update_database src/Services/Inventory/PosCafe.Inventory.Infrastructure/PosCafe.Inventory.Infrastructure.csproj src/Services/Inventory/PosCafe.Inventory.Api/PosCafe.Inventory.Api.csproj Inventory
update_database src/Gateway/PosCafe.ApiGateway/PosCafe.ApiGateway.csproj src/Gateway/PosCafe.ApiGateway/PosCafe.ApiGateway.csproj 'Gateway operations'

echo 'All migrations applied successfully.'

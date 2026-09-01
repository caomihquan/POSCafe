using System.Security.Claims;

namespace PosCafe.ServiceDefaults;

public static class StoreAuthorization
{
    public static bool CanAccessStore(this ClaimsPrincipal principal, Guid storeId) =>
        principal.IsInRole("admin") || principal.FindAll("store_id").Any(claim => Guid.TryParse(claim.Value, out var assignedStore) && assignedStore == storeId);
}

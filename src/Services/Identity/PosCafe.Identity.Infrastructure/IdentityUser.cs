using Microsoft.AspNetCore.Identity;

namespace PosCafe.Identity.Infrastructure;

public sealed class IdentityUser : IdentityUser<Guid>
{
    public string DisplayName { get; set; } = string.Empty;
}

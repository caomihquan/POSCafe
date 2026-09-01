namespace PosCafe.Identity.Infrastructure;

public sealed class UserStoreAssignment
{
    public Guid UserId { get; set; }
    public Guid StoreId { get; set; }
    public DateTime AssignedAtUtc { get; set; }
    public bool IsActive { get; set; } = true;
}

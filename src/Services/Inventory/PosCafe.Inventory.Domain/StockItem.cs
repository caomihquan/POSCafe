using BuildingBlocks.Exceptions;

namespace PosCafe.Inventory.Domain;

public sealed class StockItem
{
    private StockItem() { }
    public Guid Id { get; private set; }
    public Guid StoreId { get; private set; }
    public Guid ProductId { get; private set; }
    public decimal Quantity { get; private set; }
    public decimal ReservedQuantity { get; private set; }
    public int Version { get; private set; }
    public DateTime UpdatedAtUtc { get; private set; }
    public decimal AvailableQuantity => Quantity - ReservedQuantity;

    public StockItem(Guid storeId, Guid productId, decimal quantity = 0)
    {
        if (storeId == Guid.Empty || productId == Guid.Empty) throw new ValidationException("Store and product are required.");
        if (quantity < 0) throw new ValidationException("Quantity cannot be negative.");
        Id = Guid.NewGuid(); StoreId = storeId; ProductId = productId; Quantity = quantity; UpdatedAtUtc = DateTime.UtcNow;
    }

    public void Receive(decimal quantity)
    {
        if (quantity <= 0) throw new ValidationException("Received quantity must be positive.");
        Quantity += quantity; Version++; UpdatedAtUtc = DateTime.UtcNow;
    }

    public void Adjust(decimal quantity)
    {
        if (quantity < 0 || quantity > Quantity) throw new ValidationException("Adjustment would make stock invalid.");
        Quantity = quantity; ReservedQuantity = Math.Min(ReservedQuantity, quantity); Version++; UpdatedAtUtc = DateTime.UtcNow;
    }

    public void Reserve(decimal quantity)
    {
        if (quantity <= 0 || quantity > AvailableQuantity) throw new ConflictException("Insufficient available stock.");
        ReservedQuantity += quantity; Version++; UpdatedAtUtc = DateTime.UtcNow;
    }

    public void Release(decimal quantity)
    {
        if (quantity <= 0 || quantity > ReservedQuantity) throw new ValidationException("Release quantity exceeds reserved stock.");
        ReservedQuantity -= quantity; Version++; UpdatedAtUtc = DateTime.UtcNow;
    }
}

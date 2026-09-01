using BuildingBlocks.Exceptions;

namespace PosCafe.Order.Domain;

public sealed class OrderLine
{
    private OrderLine() { }
    private OrderLine(Guid id, Guid productId, string productName, decimal unitPrice, int quantity) { Id = id; ProductId = productId; ProductName = productName; UnitPrice = unitPrice; Quantity = quantity; }
    public Guid Id { get; private set; }
    public Guid ProductId { get; private set; }
    public string ProductName { get; private set; } = string.Empty;
    public decimal UnitPrice { get; private set; }
    public int Quantity { get; private set; }
    public decimal Total => UnitPrice * Quantity;
    public static OrderLine Create(Guid productId, string productName, decimal unitPrice, int quantity)
    {
        if (productId == Guid.Empty) throw new ValidationException("Product is required.");
        if (string.IsNullOrWhiteSpace(productName)) throw new ValidationException("Product name is required.");
        if (unitPrice < 0) throw new ValidationException("Unit price cannot be negative.");
        if (quantity <= 0) throw new ValidationException("Quantity must be greater than zero.");
        return new OrderLine(Guid.NewGuid(), productId, productName.Trim(), unitPrice, quantity);
    }
}

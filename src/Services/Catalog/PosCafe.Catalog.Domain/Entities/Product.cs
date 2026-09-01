namespace PosCafe.Catalog.Domain.Entities;

public class Product
{
    public Guid Id { get; private set; }

    public Guid CategoryId { get; private set; }

    public string Name { get; private set; } = null!;

    public decimal Price { get; private set; }

    public bool IsActive { get; private set; }

    private Product()
    {
    }

    public Product(
        Guid categoryId,
        string name,
        decimal price)
    {
        Id = Guid.NewGuid();
        CategoryId = categoryId;
        Name = name;
        Price = price;
        IsActive = true;
    }

    public void UpdatePrice(decimal price)
    {
        Price = price;
    }

    public void Deactivate()
    {
        IsActive = false;
    }
}
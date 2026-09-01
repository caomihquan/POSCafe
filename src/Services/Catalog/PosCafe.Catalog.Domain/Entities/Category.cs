namespace PosCafe.Catalog.Domain.Entities;

public class Category
{
    public Guid Id { get; private set; }

    public string Name { get; private set; } = null!;

    public bool IsActive { get; private set; }

    private Category()
    {
    }

    public Category(string name)
    {
        Id = Guid.NewGuid();
        Name = name;
        IsActive = true;
    }

    public void UpdateName(string name)
    {
        Name = name;
    }

    public void Deactivate()
    {
        IsActive = false;
    }
}
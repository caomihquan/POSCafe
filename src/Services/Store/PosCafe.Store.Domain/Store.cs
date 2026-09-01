using BuildingBlocks.Exceptions;

namespace PosCafe.Store.Domain;

public sealed class Store
{
    private Store() { }
    public Guid Id { get; private set; }
    public string Name { get; private set; } = string.Empty;
    public string Code { get; private set; } = string.Empty;
    public string TimeZone { get; private set; } = "Asia/Ho_Chi_Minh";
    public bool IsActive { get; private set; }
    public DateTime CreatedAtUtc { get; private set; }

    public Store(string code, string name, string timeZone = "Asia/Ho_Chi_Minh")
    {
        if (string.IsNullOrWhiteSpace(code)) throw new ValidationException("Store code is required.");
        if (string.IsNullOrWhiteSpace(name)) throw new ValidationException("Store name is required.");
        Id = Guid.NewGuid(); Code = code.Trim().ToUpperInvariant(); Name = name.Trim();
        TimeZone = string.IsNullOrWhiteSpace(timeZone) ? "Asia/Ho_Chi_Minh" : timeZone.Trim();
        IsActive = true; CreatedAtUtc = DateTime.UtcNow;
    }

    public void Update(string name, string timeZone)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ValidationException("Store name is required.");
        Name = name.Trim();
        if (!string.IsNullOrWhiteSpace(timeZone)) TimeZone = timeZone.Trim();
    }

    public void Deactivate() => IsActive = false;
}

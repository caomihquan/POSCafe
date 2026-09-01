namespace BuildingBlocks.Exceptions;

public sealed class NotFoundException : DomainException
{
    public NotFoundException(string resource, object key)
        : base("not_found", $"{resource} with key '{key}' was not found.")
    {
        Resource = resource;
        Key = key;
    }

    public string Resource { get; }

    public object Key { get; }
}

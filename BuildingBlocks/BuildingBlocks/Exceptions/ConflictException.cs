namespace BuildingBlocks.Exceptions;

public sealed class ConflictException : DomainException
{
    public ConflictException(string message)
        : base("conflict", message)
    {
    }
}

namespace BuildingBlocks.Exceptions;

public sealed class ForbiddenException : DomainException
{
    public ForbiddenException(string message = "You do not have permission to perform this operation.")
        : base("forbidden", message)
    {
    }
}

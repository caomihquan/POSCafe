namespace BuildingBlocks.Exceptions;

public sealed class UnauthorizedException : DomainException
{
    public UnauthorizedException(string message = "Authentication is required.")
        : base("unauthorized", message)
    {
    }
}

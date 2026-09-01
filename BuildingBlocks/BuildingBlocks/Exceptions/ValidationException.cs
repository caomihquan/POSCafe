namespace BuildingBlocks.Exceptions;

public sealed class ValidationException : DomainException
{
    public ValidationException(string message, IReadOnlyDictionary<string, string[]>? errors = null)
        : base("validation_error", message) => Errors = errors ?? new Dictionary<string, string[]>();

    public IReadOnlyDictionary<string, string[]> Errors { get; }
}

using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace BuildingBlocks.Messaging;

public static class Idempotency
{
    public const int MaxKeyLength = 200;

    public static string ValidateKey(string? key)
    {
        if (string.IsNullOrWhiteSpace(key) || key.Length > MaxKeyLength)
            throw new ArgumentException($"Idempotency key is required and must be at most {MaxKeyLength} characters.", nameof(key));
        return key.Trim();
    }

    public static string Hash<T>(T request) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(request))));

    public static bool Matches(string storedHash, string requestHash)
    {
        var matches = CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(storedHash), Encoding.UTF8.GetBytes(requestHash));
        if (!matches) MessagingMetrics.IdempotencyConflicts.Add(1);
        return matches;
    }
}

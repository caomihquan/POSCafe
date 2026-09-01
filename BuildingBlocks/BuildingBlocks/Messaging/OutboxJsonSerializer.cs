using System.Text.Json;

namespace BuildingBlocks.Messaging;

public static class OutboxJsonSerializer
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public static string Serialize<T>(T value) => JsonSerializer.Serialize(value, Options);
}

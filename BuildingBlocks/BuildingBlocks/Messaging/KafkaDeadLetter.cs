using System.Security.Cryptography;
using System.Text;
using Confluent.Kafka;

namespace BuildingBlocks.Messaging;

public static class KafkaDeadLetter
{
    public static Message<string, string> Create(ConsumeResult<string, string> source, string service, string reason)
    {
        var eventId = ReadHeader(source.Message.Headers, "event-id");
        if (!Guid.TryParse(eventId, out var parsedEventId))
        {
            parsedEventId = CreateSyntheticEventId(source);
        }

        var headers = new Headers();
        foreach (var header in source.Message.Headers)
        {
            if (header.Key is "event-id" or "poison-message" or "dead-letter-reason" or "dead-lettered-by" or "original-topic" or "original-partition" or "original-offset")
                continue;
            headers.Add(header.Key, header.GetValueBytes());
        }
        headers.Add("event-id", Encoding.UTF8.GetBytes(parsedEventId.ToString()));
        if (!string.IsNullOrWhiteSpace(eventId)) headers.Add("original-event-id", Encoding.UTF8.GetBytes(eventId));
        headers.Add("poison-message", Encoding.UTF8.GetBytes("true"));
        headers.Add("dead-letter-reason", Encoding.UTF8.GetBytes(reason));
        headers.Add("dead-lettered-by", Encoding.UTF8.GetBytes(service));
        headers.Add("original-topic", Encoding.UTF8.GetBytes(source.Topic));
        headers.Add("original-partition", Encoding.UTF8.GetBytes(source.Partition.Value.ToString()));
        headers.Add("original-offset", Encoding.UTF8.GetBytes(source.Offset.Value.ToString()));

        return new Message<string, string> { Key = source.Message.Key, Value = source.Message.Value, Headers = headers };
    }

    private static Guid CreateSyntheticEventId(ConsumeResult<string, string> source)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes($"{source.Topic}:{source.Partition.Value}:{source.Offset.Value}"));
        return new Guid(bytes[..16]);
    }

    private static string? ReadHeader(Headers headers, string key) =>
        headers.FirstOrDefault(x => x.Key == key) is { } header ? Encoding.UTF8.GetString(header.GetValueBytes()) : null;
}

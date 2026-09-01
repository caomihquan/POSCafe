using System.Text;
using Confluent.Kafka;

var arguments = ParseArguments(args);
var bootstrapServers = Required(arguments, "bootstrap-servers");
var sourceTopic = Required(arguments, "source-topic");
var targetTopic = Required(arguments, "target-topic");
var eventId = Guid.Parse(Required(arguments, "event-id"));

using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = $"poscafe-dlq-replay-{Guid.NewGuid():N}",
    EnableAutoCommit = false,
    AutoOffsetReset = AutoOffsetReset.Earliest
}).Build();
using var producer = new ProducerBuilder<string, string>(new ProducerConfig
{
    BootstrapServers = bootstrapServers,
    Acks = Acks.All,
    EnableIdempotence = true
}).Build();

consumer.Subscribe(sourceTopic);
Console.WriteLine($"Searching {sourceTopic} for event {eventId}...");
while (true)
{
    var result = consumer.Consume(TimeSpan.FromSeconds(30));
    if (result is null) throw new TimeoutException("The event was not found within the polling interval.");
    var idHeader = result.Message.Headers.FirstOrDefault(x => x.Key == "event-id");
    if (idHeader is null || !Guid.TryParse(Encoding.UTF8.GetString(idHeader.GetValueBytes()), out var currentId) || currentId != eventId)
        continue;

    var replayHeaders = new Headers();
    foreach (var header in result.Message.Headers)
        replayHeaders.Add(header.Key, header.GetValueBytes());

    await producer.ProduceAsync(targetTopic, new Message<string, string>
    {
        Key = result.Message.Key,
        Value = result.Message.Value,
        Headers = replayHeaders
    });
    producer.Flush(TimeSpan.FromSeconds(10));
    consumer.Commit(result);
    Console.WriteLine($"Replayed event {eventId} to {targetTopic}.");
    break;
}

static Dictionary<string, string> ParseArguments(string[] args)
{
    var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    for (var index = 0; index < args.Length; index++)
    {
        if (!args[index].StartsWith("--", StringComparison.Ordinal) || index + 1 >= args.Length)
            throw new ArgumentException("Arguments must use --name value format.");
        values[args[index][2..]] = args[++index];
    }
    return values;
}

static string Required(IReadOnlyDictionary<string, string> values, string name) =>
    values.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value)
        ? value
        : throw new ArgumentException($"Missing required argument --{name}.");

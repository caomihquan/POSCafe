using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Xunit;

namespace PosCafe.IntegrationTests;

public sealed class KafkaMessagingTests
{
    [Fact]
    public async Task Producer_and_consumer_preserve_event_contract_and_commit_offset()
    {
        var bootstrapServers = Environment.GetEnvironmentVariable("POSCAFE_KAFKA_BOOTSTRAP_SERVERS")
            ?? throw new InvalidOperationException("POSCAFE_KAFKA_BOOTSTRAP_SERVERS is required for Kafka integration tests.");
        var topic = $"poscafe.integration.{Guid.NewGuid():N}";
        var eventId = Guid.NewGuid();
        var correlationId = Guid.NewGuid().ToString("N");

        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();
        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = "order-1",
            Value = "{\"event\":\"OrderConfirmed.v1\"}",
            Headers = new Headers
            {
                { "event-id", Encoding.UTF8.GetBytes(eventId.ToString()) },
                { "event-type", Encoding.UTF8.GetBytes("OrderConfirmed.v1") },
                { "correlation-id", Encoding.UTF8.GetBytes(correlationId) }
            }
        });

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"poscafe-integration-{Guid.NewGuid():N}",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();
        consumer.Subscribe(topic);
        var result = consumer.Consume(TimeSpan.FromSeconds(15));

        Assert.NotNull(result);
        Assert.Equal("order-1", result.Message.Key);
        Assert.Equal("{\"event\":\"OrderConfirmed.v1\"}", result.Message.Value);
        Assert.Equal(eventId.ToString(), Encoding.UTF8.GetString(result.Message.Headers.GetLastBytes("event-id")));
        Assert.Equal("OrderConfirmed.v1", Encoding.UTF8.GetString(result.Message.Headers.GetLastBytes("event-type")));
        Assert.Equal(correlationId, Encoding.UTF8.GetString(result.Message.Headers.GetLastBytes("correlation-id")));

        consumer.Commit(result);
        consumer.Close();

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        await admin.DeleteTopicsAsync([topic]);
    }
}

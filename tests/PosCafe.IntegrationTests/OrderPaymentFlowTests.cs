using System.Text;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using PosCafe.Payment.Infrastructure.Messaging;
using PosCafe.Payment.Infrastructure.Persistence;
using Xunit;

namespace PosCafe.IntegrationTests;

public sealed class OrderPaymentFlowTests
{
    [Fact]
    public async Task Order_confirmed_flows_through_kafka_into_payment_projection_once()
    {
        var bootstrapServers = Required("POSCAFE_KAFKA_BOOTSTRAP_SERVERS");
        var connectionString = Required("POSCAFE_PAYMENT_INTEGRATION_CONNECTION");
        var topic = $"poscafe.e2e.order.{Guid.NewGuid():N}";
        var orderId = Guid.NewGuid();
        var storeId = Guid.NewGuid();
        var eventId = Guid.NewGuid();
        var payload = $"{{\"OrderId\":\"{orderId}\",\"StoreId\":\"{storeId}\",\"Total\":42.50,\"OccurredAt\":\"2026-08-31T00:00:00+00:00\"}}";

        using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers, Acks = Acks.All, EnableIdempotence = true }).Build();
        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = orderId.ToString(),
            Value = payload,
            Headers = new Headers
            {
                { "event-id", Encoding.UTF8.GetBytes(eventId.ToString()) },
                { "event-type", Encoding.UTF8.GetBytes("OrderConfirmed.v1") }
            }
        });

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = $"poscafe-e2e-{Guid.NewGuid():N}", EnableAutoCommit = false, AutoOffsetReset = AutoOffsetReset.Earliest }).Build();
        consumer.Subscribe(topic);
        var message = consumer.Consume(TimeSpan.FromSeconds(15)) ?? throw new TimeoutException("Order event was not consumed.");

        var options = new DbContextOptionsBuilder<PaymentDbContext>().UseNpgsql(connectionString).Options;
        await using var db = new PaymentDbContext(options);
        await db.Database.MigrateAsync();
        var handler = new OrderEventHandler();
        await using (var transaction = await db.Database.BeginTransactionAsync())
        {
            Assert.True(await handler.HandleAsync(db, eventId, "payment.order-events.v1", "OrderConfirmed.v1", message.Message.Value, CancellationToken.None));
            await transaction.CommitAsync();
        }

        await using (var duplicateTransaction = await db.Database.BeginTransactionAsync())
        {
            Assert.False(await handler.HandleAsync(db, eventId, "payment.order-events.v1", "OrderConfirmed.v1", message.Message.Value, CancellationToken.None));
            await duplicateTransaction.CommitAsync();
        }

        var projection = await db.OrderProjections.SingleAsync(x => x.OrderId == orderId);
        Assert.Equal(storeId, projection.StoreId);
        Assert.Equal(42.50m, projection.Total);
        Assert.Equal(1, await db.InboxMessages.CountAsync(x => x.EventId == eventId));
        consumer.Commit(message);
        consumer.Close();
    }

    private static string Required(string name) => Environment.GetEnvironmentVariable(name)
        ?? throw new InvalidOperationException($"{name} is required for integration tests.");
}

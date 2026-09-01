using System.Text;
using Confluent.Kafka;

namespace PosCafe.ApiGateway;

public sealed record DlqReplayRequest(string SourceTopic, string TargetTopic, Guid EventId);
public sealed record DlqRoute(string SourceTopic, string TargetTopic, string Consumer, IReadOnlyCollection<string> RequiredRoles);

public sealed class DlqManagementService(IProducer<string, string> producer, string bootstrapServers)
{
    public static readonly IReadOnlyCollection<DlqRoute> Routes =
    [
        new("pos.order.events.dlq", "pos.order.events", "order", ["manager", "order-operator"]),
        new("pos.payment.order-events.dlq", "pos.order.events", "payment", ["manager", "payment-operator"]),
        new("pos.inventory.order-events.dlq", "pos.order.events", "inventory", ["store-manager", "inventory-manager"]),
        new("pos.inventory.events.dlq", "pos.inventory.events", "inventory", ["manager", "inventory-manager"]),
        new("pos.reporting.order-events.dlq", "pos.order.events", "reporting", ["manager"])
    ];

    public static bool CanAccess(DlqRoute route, System.Security.Claims.ClaimsPrincipal principal) =>
        principal.IsInRole("admin") || route.RequiredRoles.Any(principal.IsInRole);

    public async Task<(bool Found, string? Topic, long? Offset)> ReplayAsync(DlqReplayRequest request, CancellationToken cancellationToken, Func<CancellationToken, Task>? renewLease = null)
    {
        var route = Routes.SingleOrDefault(x => x.SourceTopic == request.SourceTopic && x.TargetTopic == request.TargetTopic);
        if (route is null) throw new InvalidOperationException("The DLQ route is not allowed.");

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"poscafe-dlq-management-{Guid.NewGuid():N}",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = false
        };
        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(route.SourceTopic);
        var nextLeaseRenewal = DateTime.UtcNow.AddMinutes(1);
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(2));
                if (renewLease is not null && DateTime.UtcNow >= nextLeaseRenewal)
                {
                    await renewLease(cancellationToken);
                    nextLeaseRenewal = DateTime.UtcNow.AddMinutes(1);
                }
                if (result is null) continue;
                if (renewLease is not null)
                    await renewLease(cancellationToken);
                var header = result.Message.Headers.FirstOrDefault(x => x.Key == "event-id");
                if (header is null || !Guid.TryParse(Encoding.UTF8.GetString(header.GetValueBytes()), out var currentId) || currentId != request.EventId)
                    continue;

                await producer.ProduceAsync(route.TargetTopic, new Message<string, string>
                {
                    Key = result.Message.Key,
                    Value = result.Message.Value,
                    Headers = CopyHeaders(result.Message.Headers, request.EventId)
                }, cancellationToken);
                consumer.Commit(result);
                return (true, result.Topic, result.Offset.Value);
            }
        }
        finally
        {
            consumer.Close();
        }

        return (false, null, null);
    }

    private static Headers CopyHeaders(Headers source, Guid eventId)
    {
        var headers = new Headers();
        foreach (var header in source) headers.Add(header.Key, header.GetValueBytes());
        headers.Add("replayed-at", Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString("O")));
        headers.Add("replayed-event-id", Encoding.UTF8.GetBytes(eventId.ToString()));
        return headers;
    }
}

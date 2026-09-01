using BuildingBlocks.Messaging;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using PosCafe.Payment.Application;
using PosCafe.Payment.Infrastructure;
using PosCafe.Payment.Infrastructure.Messaging;
using PosCafe.Payment.Infrastructure.Persistence;
using Xunit;

namespace PosCafe.Payment.Tests;

public sealed class OutboxIntegrationTests
{
    [Fact]
    public async Task Inbox_accepts_an_event_only_once_per_consumer()
    {
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptionsBuilder<PaymentDbContext>().UseSqlite(connection).Options;
        await using var db = new PaymentDbContext(options);
        await db.Database.EnsureCreatedAsync();
        var eventId = Guid.NewGuid();

        Assert.True(await InboxProcessor.TryStartAsync(db, eventId, "test-consumer", CancellationToken.None));
        await InboxProcessor.MarkProcessedAsync(db, eventId, "test-consumer", CancellationToken.None);
        Assert.False(await InboxProcessor.TryStartAsync(db, eventId, "test-consumer", CancellationToken.None));
        Assert.Equal(1, await db.InboxMessages.CountAsync());
    }

    [Fact]
    public async Task Inbox_allows_retry_for_an_unprocessed_event_and_persists_attempts()
    {
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptionsBuilder<PaymentDbContext>().UseSqlite(connection).Options;
        await using var db = new PaymentDbContext(options);
        await db.Database.EnsureCreatedAsync();
        var eventId = Guid.NewGuid();

        Assert.Equal(1, await InboxProcessor.RegisterAttemptAsync(db, eventId, "test-consumer", CancellationToken.None));
        Assert.Equal(2, await InboxProcessor.RegisterAttemptAsync(db, eventId, "test-consumer", CancellationToken.None));
        Assert.True(await InboxProcessor.TryStartAsync(db, eventId, "test-consumer", CancellationToken.None));
        Assert.Equal(2, (await db.InboxMessages.SingleAsync()).Attempts);
    }

    [Fact]
    public async Task Outbox_and_payment_are_persisted_in_the_same_database()
    {
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptionsBuilder<PaymentDbContext>().UseSqlite(connection).Options;
        await using var db = new PaymentDbContext(options);
        await db.Database.EnsureCreatedAsync();
        var payment = PosCafe.Payment.Domain.Payment.Create(Guid.NewGuid(), 25m, "Card");
        db.Payments.Add(payment);
        foreach (var domainEvent in payment.DequeueDomainEvents())
            db.OutboxMessages.Add(new OutboxMessage { Id = Guid.NewGuid(), AggregateId = payment.Id.ToString(), EventType = domainEvent.GetType().Name, Payload = "{}", OccurredOnUtc = domainEvent.OccurredAt.UtcDateTime });
        await db.SaveChangesAsync();

        Assert.Equal(1, await db.Payments.CountAsync());
        Assert.Equal(1, await db.OutboxMessages.CountAsync());
    }

    [Fact]
    public async Task Payment_command_creates_a_correlated_outbox_event()
    {
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptionsBuilder<PaymentDbContext>().UseSqlite(connection).Options;
        await using var db = new PaymentDbContext(options);
        await db.Database.EnsureCreatedAsync();
        var service = new PaymentCommandService(db);

        await service.CreateAsync(new CreatePaymentCommand(Guid.NewGuid(), 25m, "Card"), CancellationToken.None);

        Assert.False(string.IsNullOrWhiteSpace(await db.OutboxMessages.Select(x => x.CorrelationId).SingleAsync()));
    }

    [Fact]
    public async Task Order_projection_is_keyed_by_order_id()
    {
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptionsBuilder<PaymentDbContext>().UseSqlite(connection).Options;
        await using var db = new PaymentDbContext(options);
        await db.Database.EnsureCreatedAsync();
        var orderId = Guid.NewGuid();
        db.OrderProjections.Add(new PaymentOrderProjection { OrderId = orderId, StoreId = Guid.NewGuid(), Total = 12m });
        await db.SaveChangesAsync();
        Assert.Equal(12m, (await db.OrderProjections.SingleAsync(x => x.OrderId == orderId)).Total);
    }

    [Fact]
    public async Task Order_confirmed_handler_updates_projection_once_with_inbox_idempotency()
    {
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptionsBuilder<PaymentDbContext>().UseSqlite(connection).Options;
        await using var db = new PaymentDbContext(options);
        await db.Database.EnsureCreatedAsync();
        var orderId = Guid.NewGuid();
        var storeId = Guid.NewGuid();
        var eventId = Guid.NewGuid();
        const string payload = "{\"OrderId\":\"00000000-0000-0000-0000-000000000001\",\"StoreId\":\"00000000-0000-0000-0000-000000000002\",\"Total\":42.50,\"OccurredAt\":\"2026-08-31T00:00:00+00:00\"}";
        var actualPayload = payload.Replace("00000000-0000-0000-0000-000000000001", orderId.ToString()).Replace("00000000-0000-0000-0000-000000000002", storeId.ToString());
        var handler = new OrderEventHandler();

        Assert.True(await handler.HandleAsync(db, eventId, "payment.order-events.v1", "OrderConfirmed.v1", actualPayload, CancellationToken.None));
        Assert.False(await handler.HandleAsync(db, eventId, "payment.order-events.v1", "OrderConfirmed.v1", actualPayload, CancellationToken.None));
        Assert.Equal(42.50m, (await db.OrderProjections.SingleAsync(x => x.OrderId == orderId)).Total);
        Assert.Equal(1, await db.InboxMessages.CountAsync());
    }

    [Fact]
    public async Task Invalid_order_event_rolls_back_inbox_and_can_be_retried()
    {
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptionsBuilder<PaymentDbContext>().UseSqlite(connection).Options;
        await using var db = new PaymentDbContext(options);
        await db.Database.EnsureCreatedAsync();
        var eventId = Guid.NewGuid();
        var handler = new OrderEventHandler();

        await using (var failedTransaction = await db.Database.BeginTransactionAsync())
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => handler.HandleAsync(db, eventId, "payment.order-events.v1", "OrderConfirmed.v1", "invalid-json", CancellationToken.None));
            await failedTransaction.RollbackAsync();
            db.ChangeTracker.Clear();
        }

        Assert.Equal(0, await db.InboxMessages.CountAsync());
        var orderId = Guid.NewGuid();
        var storeId = Guid.NewGuid();
        var validPayload = $"{{\"OrderId\":\"{orderId}\",\"StoreId\":\"{storeId}\",\"Total\":10.00,\"OccurredAt\":\"2026-08-31T00:00:00+00:00\"}}";
        await using (var retryTransaction = await db.Database.BeginTransactionAsync())
        {
            Assert.True(await handler.HandleAsync(db, eventId, "payment.order-events.v1", "OrderConfirmed.v1", validPayload, CancellationToken.None));
            await retryTransaction.CommitAsync();
        }

        Assert.Equal(1, await db.InboxMessages.CountAsync());
        Assert.Equal(10m, (await db.OrderProjections.SingleAsync(x => x.OrderId == orderId)).Total);
    }
}

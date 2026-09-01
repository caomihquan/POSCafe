using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using PosCafe.Order.Application;
using PosCafe.Order.Infrastructure;
using PosCafe.Order.Infrastructure.Persistence;
using Xunit;

namespace PosCafe.Order.Tests;

public sealed class OrderCommandIntegrationTests
{
    [Fact]
    public async Task Create_persists_order_and_outbox_event_together()
    {
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptionsBuilder<OrderDbContext>().UseSqlite(connection).Options;
        await using var db = new OrderDbContext(options);
        await db.Database.EnsureCreatedAsync();
        var service = new OrderCommandService(db);
        var command = new CreateOrderCommand(Guid.NewGuid(), "DineIn", [new(Guid.NewGuid(), "Espresso", 3m, 2)]);

        var result = await service.CreateAsync(command, CancellationToken.None);

        Assert.Equal("Draft", result.Status);
        Assert.Equal(1, await db.Orders.CountAsync());
        Assert.Equal(1, await db.OutboxMessages.CountAsync());
        Assert.False(string.IsNullOrWhiteSpace(await db.OutboxMessages.Select(x => x.CorrelationId).SingleAsync()));
    }
}

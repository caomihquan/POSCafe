using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace PosCafe.Order.Infrastructure.Persistence;

public sealed class OrderDbContextFactory : IDesignTimeDbContextFactory<OrderDbContext>
{
    public OrderDbContext CreateDbContext(string[] args)
    {
        var options = new DbContextOptionsBuilder<OrderDbContext>()
            .UseNpgsql(Environment.GetEnvironmentVariable("ConnectionStrings__orderdb") ?? Environment.GetEnvironmentVariable("POSCAFE_ORDER_CONNECTION") ?? "Host=localhost;Database=orderdb;Username=postgres;Password=postgres")
            .Options;
        return new OrderDbContext(options);
    }
}

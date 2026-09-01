using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace PosCafe.Payment.Infrastructure.Persistence;

public sealed class PaymentDbContextFactory : IDesignTimeDbContextFactory<PaymentDbContext>
{
    public PaymentDbContext CreateDbContext(string[] args)
    {
        var options = new DbContextOptionsBuilder<PaymentDbContext>()
            .UseNpgsql(Environment.GetEnvironmentVariable("ConnectionStrings__paymentdb") ?? Environment.GetEnvironmentVariable("POSCAFE_PAYMENT_CONNECTION") ?? "Host=localhost;Database=paymentdb;Username=postgres;Password=postgres")
            .Options;
        return new PaymentDbContext(options);
    }
}

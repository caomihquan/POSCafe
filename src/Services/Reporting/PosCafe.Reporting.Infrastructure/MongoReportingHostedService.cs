using Microsoft.Extensions.Hosting;

namespace PosCafe.Reporting.Infrastructure;

public sealed class MongoReportingHostedService(MongoReportingRepository repository) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) => repository.EnsureIndexesAsync(cancellationToken);
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

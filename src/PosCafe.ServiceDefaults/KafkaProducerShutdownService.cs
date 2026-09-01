using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace PosCafe.ServiceDefaults;

public sealed class KafkaProducerShutdownService(
    IProducer<string, string> producer,
    ILogger<KafkaProducerShutdownService> logger) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            producer.Flush(TimeSpan.FromSeconds(10));
            logger.LogInformation("Kafka producer flushed successfully during shutdown.");
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Kafka producer flush failed during shutdown; in-flight messages remain recoverable through the outbox or replay workflow.");
        }

        return Task.CompletedTask;
    }
}

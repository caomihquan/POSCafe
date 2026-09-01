using System.Text;
using System.Text.Json;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using BuildingBlocks.Messaging;

namespace BuildingBlocks.Observability;

public sealed class AuditArchiveOptions
{
    public bool Enabled { get; set; }
    public string ConnectionString { get; set; } = string.Empty;
    public string ContainerName { get; set; } = "audit-archive";
    public string ServiceName { get; set; } = "unknown";
}

public sealed class AuditArchiveClient(AuditArchiveOptions options)
{
    public async Task ArchiveAsync(IReadOnlyCollection<AuditEntry> entries, CancellationToken cancellationToken)
    {
        if (!options.Enabled || entries.Count == 0) return;
        if (string.IsNullOrWhiteSpace(options.ConnectionString)) throw new InvalidOperationException("Audit archive is enabled but connection string is missing.");
        try
        {
            var client = new BlobContainerClient(options.ConnectionString, options.ContainerName);
            await client.CreateIfNotExistsAsync(cancellationToken: cancellationToken);
            var firstDate = entries.Min(x => x.OccurredAtUtc).ToString("yyyy-MM-dd");
            var blob = client.GetBlobClient($"{options.ServiceName}/{firstDate}/{DateTime.UtcNow:yyyyMMddHHmmssfff}-{Guid.NewGuid():N}.jsonl");
            var content = string.Join(Environment.NewLine, entries.Select(entry => JsonSerializer.Serialize(entry))) + Environment.NewLine;
            await using var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));
            await blob.UploadAsync(stream, new BlobUploadOptions { HttpHeaders = new BlobHttpHeaders { ContentType = "application/x-ndjson" }, Metadata = new Dictionary<string, string> { ["service"] = options.ServiceName, ["record-count"] = entries.Count.ToString() } }, cancellationToken);
            var tags = new KeyValuePair<string, object?>[] { new("service", options.ServiceName) };
            MessagingMetrics.ArchiveSucceeded.Add(1, tags);
            MessagingMetrics.ArchivedRecords.Add(entries.Count, tags);
        }
        catch
        {
            MessagingMetrics.ArchiveFailures.Add(1, new KeyValuePair<string, object?>("service", options.ServiceName));
            throw;
        }
    }
}

namespace BuildingBlocks.Messaging;

public sealed class OutboxOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = string.Empty;
    public string InputTopic { get; set; } = string.Empty;
    public string ConsumerGroup { get; set; } = string.Empty;
    public int BatchSize { get; set; } = 50;
    public int MaxAttempts { get; set; } = 10;
    public int LeaseSeconds { get; set; } = 60;
    public int PollSeconds { get; set; } = 5;
    public int ConsumerRetrySeconds { get; set; } = 5;
    public int ConsumerMaxAttempts { get; set; } = 5;
    public int RetryMaxSeconds { get; set; } = 300;
    public string DeadLetterTopic { get; set; } = string.Empty;
}

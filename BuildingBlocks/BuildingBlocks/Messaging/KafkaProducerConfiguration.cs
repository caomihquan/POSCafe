using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace BuildingBlocks.Messaging;

public static class KafkaProducerConfiguration
{
    public static ProducerConfig Create(string bootstrapServers) => new()
    {
        BootstrapServers = bootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = true,
        MessageSendMaxRetries = 10,
        RetryBackoffMs = 500,
        RequestTimeoutMs = 30_000,
        MessageTimeoutMs = 120_000,
        EnableDeliveryReports = true,
        AllowAutoCreateTopics = false,
        CompressionType = CompressionType.Snappy
    };

    public static ProducerConfig Create(IConfiguration configuration, string bootstrapServers)
    {
        var config = Create(bootstrapServers);
        ApplySecurity(config, configuration.GetSection("Kafka:Security"));
        return config;
    }

    public static void ApplySecurity(ClientConfig config, IConfigurationSection security)
    {
        if (!bool.TryParse(security["Enabled"], out var enabled) || !enabled) return;
        if (!Enum.TryParse<SecurityProtocol>(security["Protocol"], true, out var protocol))
            throw new InvalidOperationException("Kafka:Security:Protocol must be configured when Kafka security is enabled.");

        config.SecurityProtocol = protocol;
        if (protocol is SecurityProtocol.SaslPlaintext or SecurityProtocol.SaslSsl)
        {
            config.SaslMechanism = Enum.TryParse<SaslMechanism>(security["SaslMechanism"], true, out var mechanism)
                ? mechanism : throw new InvalidOperationException("Kafka:Security:SaslMechanism is required for SASL.");
            config.SaslUsername = security["Username"] ?? throw new InvalidOperationException("Kafka:Security:Username is required for SASL.");
            config.SaslPassword = security["Password"] ?? throw new InvalidOperationException("Kafka:Security:Password is required for SASL.");
        }

        if (protocol is SecurityProtocol.Ssl or SecurityProtocol.SaslSsl)
        {
            config.SslCaLocation = security["CaLocation"] ?? throw new InvalidOperationException("Kafka:Security:CaLocation is required for TLS.");
            config.SslCertificateLocation = security["CertificateLocation"];
            config.SslKeyLocation = security["KeyLocation"];
            config.SslKeyPassword = security["KeyPassword"];
            config.EnableSslCertificateVerification = !bool.TryParse(security["DisableCertificateVerification"], out var disabled) || !disabled;
        }
    }
}

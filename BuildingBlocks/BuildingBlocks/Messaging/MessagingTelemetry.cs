using System.Diagnostics;

namespace BuildingBlocks.Messaging;

public static class MessagingTelemetry
{
    public static readonly ActivitySource ActivitySource = new("PosCafe.Messaging", "1.0.0");
}

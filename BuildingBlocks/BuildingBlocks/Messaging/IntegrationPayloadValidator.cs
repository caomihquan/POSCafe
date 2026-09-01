using System.Globalization;
using System.Text.Json;

namespace BuildingBlocks.Messaging;

public static class IntegrationPayloadValidator
{
    public static void Validate(string eventType, string payload)
    {
        using var document = JsonDocument.Parse(payload);
        var root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object) throw new InvalidOperationException("Integration event payload must be a JSON object.");

        switch (eventType)
        {
            case "OrderCreated.v1":
                RequireGuid(root, "OrderId"); RequireGuid(root, "StoreId"); RequireTimestamp(root, "OccurredAt"); break;
            case "OrderCancelled.v1":
                RequireGuid(root, "OrderId"); RequireText(root, "Reason"); RequireTimestamp(root, "OccurredAt"); break;
            case "OrderConfirmed.v1":
                RequireGuid(root, "OrderId"); RequireGuid(root, "StoreId"); RequirePositiveNumber(root, "Total"); RequireTimestamp(root, "OccurredAt");
                if (!root.TryGetProperty("Lines", out var lines) || lines.ValueKind != JsonValueKind.Array || lines.GetArrayLength() == 0)
                    throw new InvalidOperationException("OrderConfirmed.v1 requires at least one line.");
                foreach (var line in lines.EnumerateArray()) { RequireGuid(line, "ProductId"); RequirePositiveNumber(line, "Quantity"); }
                break;
            case "PaymentCreated.v1":
            case "PaymentAuthorized.v1":
            case "PaymentRefunded.v1":
                RequireGuid(root, "PaymentId"); RequireGuid(root, "OrderId"); RequirePositiveNumber(root, "Amount"); RequireTimestamp(root, "OccurredAt"); break;
            case "PaymentAuthorizationRequested.v1":
                RequireGuid(root, "SagaId"); RequireGuid(root, "OrderId"); RequirePositiveNumber(root, "Amount"); RequireText(root, "Method"); RequireTimestamp(root, "OccurredAt"); break;
            case "PaymentRefundRequested.v1":
                RequireGuid(root, "SagaId"); RequireGuid(root, "PaymentId"); RequireGuid(root, "OrderId"); RequirePositiveNumber(root, "Amount"); RequireTimestamp(root, "OccurredAt"); break;
            case "InventoryReservationRequested.v1":
                RequireGuid(root, "SagaId"); RequireGuid(root, "OrderId"); RequireGuid(root, "StoreId"); RequirePositiveNumber(root, "Total"); RequireTimestamp(root, "OccurredAt");
                if (!root.TryGetProperty("Lines", out var requestedLines) || requestedLines.ValueKind != JsonValueKind.Array || requestedLines.GetArrayLength() == 0)
                    throw new InvalidOperationException("InventoryReservationRequested.v1 requires at least one line.");
                foreach (var line in requestedLines.EnumerateArray()) { RequireGuid(line, "ProductId"); RequirePositiveNumber(line, "Quantity"); }
                break;
            case "InventoryReserved.v1":
                RequireGuid(root, "SagaId"); RequireGuid(root, "OrderId"); RequireGuid(root, "StoreId"); RequireTimestamp(root, "OccurredAt"); break;
            case "InventoryReservationFailed.v1":
                RequireGuid(root, "SagaId"); RequireGuid(root, "OrderId"); RequireGuid(root, "StoreId"); RequireText(root, "Reason"); RequireTimestamp(root, "OccurredAt"); break;
            default: throw new InvalidOperationException($"No runtime validator registered for event type '{eventType}'.");
        }
    }

    private static void RequireGuid(JsonElement element, string name)
    {
        if (!element.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.String || !Guid.TryParse(value.GetString(), out _))
            throw new InvalidOperationException($"Integration event field '{name}' must be a UUID.");
    }

    private static void RequireTimestamp(JsonElement element, string name)
    {
        if (!element.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.String || !DateTimeOffset.TryParse(value.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out _))
            throw new InvalidOperationException($"Integration event field '{name}' must be an ISO-8601 timestamp.");
    }

    private static void RequireText(JsonElement element, string name)
    {
        if (!element.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(value.GetString()))
            throw new InvalidOperationException($"Integration event field '{name}' must be non-empty text.");
    }

    private static void RequirePositiveNumber(JsonElement element, string name)
    {
        if (!element.TryGetProperty(name, out var value) || !value.TryGetDecimal(out var number) || number <= 0)
            throw new InvalidOperationException($"Integration event field '{name}' must be greater than zero.");
    }
}

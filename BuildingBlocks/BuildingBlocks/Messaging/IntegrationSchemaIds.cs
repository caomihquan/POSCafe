namespace BuildingBlocks.Messaging;

public static class IntegrationSchemaIds
{
    public static string ForEventType(string eventType) => eventType switch
    {
        "OrderCreated.v1" => "order-created.v1",
        "OrderConfirmed.v1" => "order-confirmed.v1",
        "OrderCancelled.v1" => "order-cancelled.v1",
        "PaymentCreated.v1" => "payment-created.v1",
        "PaymentAuthorized.v1" => "payment-authorized.v1",
        "PaymentRefunded.v1" => "payment-refunded.v1",
        _ => throw new InvalidOperationException($"No registered schema for event type '{eventType}'.")
    };
}

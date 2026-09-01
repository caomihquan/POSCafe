namespace PosCafe.Payment.Application;

public sealed record CreatePaymentCommand(Guid OrderId, decimal Amount, string Method, Guid? ActorId = null);
public sealed record PaymentActionCommand(Guid PaymentId, Guid? ActorId = null);
public sealed record PaymentCommandResult(Guid PaymentId, string Status, decimal Amount, bool IdempotencyReplayed = false);

public interface IPaymentCommandService
{
    Task<PaymentCommandResult> CreateAsync(CreatePaymentCommand command, string idempotencyKey, string requestHash, CancellationToken cancellationToken);
    Task<PaymentCommandResult> AuthorizeAsync(PaymentActionCommand command, CancellationToken cancellationToken);
    Task<PaymentCommandResult> RefundAsync(PaymentActionCommand command, CancellationToken cancellationToken);
}

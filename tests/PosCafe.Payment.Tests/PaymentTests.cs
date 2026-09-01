using PosCafe.Payment.Domain;
using Xunit;
using PaymentAggregate = PosCafe.Payment.Domain.Payment;

namespace PosCafe.Payment.Tests;

public sealed class PaymentTests
{
    [Fact]
    public void Payment_must_have_positive_amount()
    {
        Assert.Throws<BuildingBlocks.Exceptions.ValidationException>(() => PaymentAggregate.Create(Guid.NewGuid(), 0, "Cash"));
    }

    [Fact]
    public void Authorize_then_refund_follows_valid_state_machine()
    {
        var payment = PaymentAggregate.Create(Guid.NewGuid(), 10m, "Card");
        payment.Authorize();
        payment.Refund();
        Assert.Equal(PaymentStatus.Refunded, payment.Status);
        Assert.Contains(payment.DomainEvents, x => x is PaymentAuthorizedDomainEvent);
        Assert.Contains(payment.DomainEvents, x => x is PaymentRefundedDomainEvent);
    }
}

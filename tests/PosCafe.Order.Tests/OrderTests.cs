using PosCafe.Order.Domain;
using Xunit;
using OrderAggregate = PosCafe.Order.Domain.Order;

namespace PosCafe.Order.Tests;

public sealed class OrderTests
{
    [Fact]
    public void Confirm_without_lines_throws_validation_exception()
    {
        var order = OrderAggregate.Create(Guid.NewGuid(), "DineIn");
        Assert.Throws<BuildingBlocks.Exceptions.ValidationException>(order.Confirm);
    }

    [Fact]
    public void Confirm_raises_event_and_calculates_subtotal()
    {
        var order = OrderAggregate.Create(Guid.NewGuid(), "TakeAway");
        order.AddLine(OrderLine.Create(Guid.NewGuid(), "Latte", 4.5m, 2));
        order.Confirm();
        Assert.Equal(OrderStatus.Confirmed, order.Status);
        Assert.Equal(9m, order.Subtotal);
        Assert.Contains(order.DomainEvents, x => x is OrderConfirmedDomainEvent);
    }

    [Fact]
    public void Cancel_requires_reason()
    {
        var order = OrderAggregate.Create(Guid.NewGuid(), "DineIn");
        Assert.Throws<BuildingBlocks.Exceptions.ValidationException>(() => order.Cancel(" "));
    }
}

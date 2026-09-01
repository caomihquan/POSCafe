namespace BuildingBlocks.Domain;

public abstract class AggregateRoot<TId>(TId id) : Entity<TId>(id)
    where TId : notnull
{
    public int Version { get; protected set; }
}

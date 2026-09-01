using Npgsql;
using Xunit;
using Xunit.Sdk;

namespace PosCafe.IntegrationTests;

public sealed class PostgresOutboxClaimTests
{
    [Fact]
    public async Task Skip_locked_allows_only_one_concurrent_outbox_claim()
    {
        var connectionString = Environment.GetEnvironmentVariable("POSCAFE_POSTGRES_CONNECTION")
            ?? throw SkipException.ForSkip("POSCAFE_POSTGRES_CONNECTION is not configured.");
        var table = $"outbox_claim_test_{Guid.NewGuid():N}";

        await using var setup = new NpgsqlConnection(connectionString);
        await setup.OpenAsync();
        await using (var command = new NpgsqlCommand($"CREATE TABLE {table} (\"Id\" uuid PRIMARY KEY, \"OccurredOnUtc\" timestamptz NOT NULL, \"Attempts\" integer NOT NULL, \"ProcessedOnUtc\" timestamptz NULL, \"DeadLetteredOnUtc\" timestamptz NULL, \"LockedUntilUtc\" timestamptz NULL); INSERT INTO {table} VALUES (@id, now(), 0, NULL, NULL, NULL);", setup))
        {
            command.Parameters.AddWithValue("id", Guid.NewGuid());
            await command.ExecuteNonQueryAsync();
        }

        try
        {
            await using var first = new NpgsqlConnection(connectionString);
            await using var second = new NpgsqlConnection(connectionString);
            await first.OpenAsync();
            await second.OpenAsync();
            await using var firstTx = await first.BeginTransactionAsync();
            await using var secondTx = await second.BeginTransactionAsync();

            var firstClaim = await ClaimAsync(first, firstTx, table);
            var secondClaim = await ClaimAsync(second, secondTx, table);

            Assert.Equal(1, firstClaim);
            Assert.Equal(0, secondClaim);
            await firstTx.RollbackAsync();
            await secondTx.RollbackAsync();
        }
        finally
        {
            await using var cleanup = new NpgsqlCommand($"DROP TABLE IF EXISTS {table};", setup);
            await cleanup.ExecuteNonQueryAsync();
        }
    }

    private static async Task<int> ClaimAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string table)
    {
        await using var command = new NpgsqlCommand($"SELECT \"Id\" FROM {table} WHERE \"ProcessedOnUtc\" IS NULL AND \"DeadLetteredOnUtc\" IS NULL AND \"Attempts\" < 10 AND (\"LockedUntilUtc\" IS NULL OR \"LockedUntilUtc\" < now()) ORDER BY \"OccurredOnUtc\" LIMIT 1 FOR UPDATE SKIP LOCKED;", connection, transaction);
        return await command.ExecuteScalarAsync() is null ? 0 : 1;
    }
}

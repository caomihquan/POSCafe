using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace PosCafe.ApiGateway.Migrations
{
    /// <inheritdoc />
    public partial class InitialOps : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "dlq_replay_records",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    IdempotencyKey = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: false),
                    EventId = table.Column<Guid>(type: "uuid", nullable: false),
                    SourceTopic = table.Column<string>(type: "character varying(250)", maxLength: 250, nullable: false),
                    TargetTopic = table.Column<string>(type: "character varying(250)", maxLength: 250, nullable: false),
                    Status = table.Column<string>(type: "character varying(30)", maxLength: 30, nullable: false),
                    ActorId = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: false),
                    CorrelationId = table.Column<string>(type: "character varying(128)", maxLength: 128, nullable: true),
                    SourceOffset = table.Column<long>(type: "bigint", nullable: true),
                    Error = table.Column<string>(type: "character varying(2000)", maxLength: 2000, nullable: true),
                    CreatedAtUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CompletedAtUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_dlq_replay_records", x => x.Id);
                });

            migrationBuilder.CreateIndex(
                name: "IX_dlq_replay_records_CreatedAtUtc_Status",
                table: "dlq_replay_records",
                columns: new[] { "CreatedAtUtc", "Status" });

            migrationBuilder.CreateIndex(
                name: "IX_dlq_replay_records_IdempotencyKey",
                table: "dlq_replay_records",
                column: "IdempotencyKey",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "dlq_replay_records");
        }
    }
}

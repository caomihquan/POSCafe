using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace PosCafe.ApiGateway.Migrations
{
    /// <inheritdoc />
    public partial class AddReplayLease : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "AttemptCount",
                table: "dlq_replay_records",
                type: "integer",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<DateTime>(
                name: "LeaseUntilUtc",
                table: "dlq_replay_records",
                type: "timestamp with time zone",
                nullable: true);

            migrationBuilder.CreateIndex(
                name: "IX_dlq_replay_records_Status_LeaseUntilUtc",
                table: "dlq_replay_records",
                columns: new[] { "Status", "LeaseUntilUtc" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_dlq_replay_records_Status_LeaseUntilUtc",
                table: "dlq_replay_records");

            migrationBuilder.DropColumn(
                name: "AttemptCount",
                table: "dlq_replay_records");

            migrationBuilder.DropColumn(
                name: "LeaseUntilUtc",
                table: "dlq_replay_records");
        }
    }
}

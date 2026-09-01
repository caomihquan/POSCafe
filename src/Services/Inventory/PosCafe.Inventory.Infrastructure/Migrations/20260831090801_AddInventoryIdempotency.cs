using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace PosCafe.Inventory.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class AddInventoryIdempotency : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "inventory_idempotency_records",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    IdempotencyKey = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: false),
                    RequestHash = table.Column<string>(type: "character varying(128)", maxLength: 128, nullable: false),
                    ResponseJson = table.Column<string>(type: "text", nullable: false),
                    CreatedAtUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_inventory_idempotency_records", x => x.Id);
                });

            migrationBuilder.CreateIndex(
                name: "IX_inventory_idempotency_records_IdempotencyKey",
                table: "inventory_idempotency_records",
                column: "IdempotencyKey",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "inventory_idempotency_records");
        }
    }
}

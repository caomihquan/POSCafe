using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace PosCafe.Order.Infrastructure.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class AddOrderFulfillmentSaga : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "order_fulfillment_sagas",
                columns: table => new
                {
                    SagaId = table.Column<Guid>(type: "uuid", nullable: false),
                    OrderId = table.Column<Guid>(type: "uuid", nullable: false),
                    StoreId = table.Column<Guid>(type: "uuid", nullable: false),
                    Total = table.Column<decimal>(type: "numeric(18,2)", precision: 18, scale: 2, nullable: false),
                    PaymentMethod = table.Column<string>(type: "character varying(40)", maxLength: 40, nullable: false),
                    Status = table.Column<string>(type: "character varying(40)", maxLength: 40, nullable: false),
                    PaymentId = table.Column<Guid>(type: "uuid", nullable: true),
                    PaymentAuthorized = table.Column<bool>(type: "boolean", nullable: false),
                    InventoryReserved = table.Column<bool>(type: "boolean", nullable: false),
                    InventoryReservationFailed = table.Column<bool>(type: "boolean", nullable: false),
                    PaymentRefundRequested = table.Column<bool>(type: "boolean", nullable: false),
                    LastError = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true),
                    CreatedAtUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    UpdatedAtUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_order_fulfillment_sagas", x => x.SagaId);
                });

            migrationBuilder.CreateIndex(
                name: "IX_order_fulfillment_sagas_OrderId",
                table: "order_fulfillment_sagas",
                column: "OrderId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_order_fulfillment_sagas_Status_UpdatedAtUtc",
                table: "order_fulfillment_sagas",
                columns: new[] { "Status", "UpdatedAtUtc" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "order_fulfillment_sagas");
        }
    }
}

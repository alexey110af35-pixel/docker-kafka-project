using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace TransactionProcessor.Migrations
{
    /// <inheritdoc />
    public partial class FixForeignKeyToInt : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "transaction_id_fk",
                table: "transaction_events",
                type: "integer",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<int>(
                name: "transaction_id_fk1",
                table: "transaction_events",
                type: "integer",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "transaction_id_fk",
                table: "transaction_events");

            migrationBuilder.DropColumn(
                name: "transaction_id_fk1",
                table: "transaction_events");
        }
    }
}

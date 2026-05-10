using Microsoft.EntityFrameworkCore;

namespace TransactionProcessor.Models;

public class TransactionDbContext : DbContext
{
    public TransactionDbContext(DbContextOptions<TransactionDbContext> options)
        : base(options)
    {
    }

    public DbSet<Transaction> Transactions { get; set; }
    public DbSet<TransactionEvent> TransactionEvents { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // Настройка индексов
        modelBuilder.Entity<Transaction>()
            .HasIndex(t => t.TransactionId)
            .IsUnique();

        modelBuilder.Entity<Transaction>()
            .HasIndex(t => t.Status);

        modelBuilder.Entity<TransactionEvent>()
            .HasIndex(e => e.TransactionId);

        modelBuilder.Entity<TransactionEvent>()
            .HasIndex(e => e.EventTime);

        // Настройка отношений
        modelBuilder.Entity<TransactionEvent>()
            .HasOne(e => e.Transaction)
            .WithMany(t => t.Events)
            .HasForeignKey(e => e.TransactionId)
            .HasPrincipalKey(t => t.TransactionId)
            .OnDelete(DeleteBehavior.Cascade);

        // Установка точности для datetime
        modelBuilder.Entity<Transaction>()
            .Property(t => t.CreatedAt)
            .HasDefaultValueSql("CURRENT_TIMESTAMP");

        modelBuilder.Entity<Transaction>()
            .Property(t => t.UpdatedAt)
            .HasDefaultValueSql("CURRENT_TIMESTAMP");

        // Конвертация для DateTime (чтобы все было в UTC)
        modelBuilder.Entity<Transaction>()
            .Property(t => t.CreatedAt)
            .HasConversion(
                v => v.ToUniversalTime(),
                v => DateTime.SpecifyKind(v, DateTimeKind.Utc));

        modelBuilder.Entity<Transaction>()
            .Property(t => t.UpdatedAt)
            .HasConversion(
                v => v.ToUniversalTime(),
                v => DateTime.SpecifyKind(v, DateTimeKind.Utc));

        modelBuilder.Entity<TransactionEvent>()
            .Property(e => e.EventTime)
            .HasConversion(
                v => v.ToUniversalTime(),
                v => DateTime.SpecifyKind(v, DateTimeKind.Utc));
    }
}
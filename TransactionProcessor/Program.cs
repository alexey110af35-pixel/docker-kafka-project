using Microsoft.EntityFrameworkCore;
using TransactionProcessor.Models;
using TransactionProcessor.Services;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<KafkaConsumerService>();
builder.Services.AddHostedService<RetryTopicConsumer>();
builder.Services.AddScoped<TransactionService>();

var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException("Database connection string not found");

builder.Services.AddDbContextFactory<TransactionDbContext>(options =>
    options.UseNpgsql(connectionString, npgsqlOptions =>
    {
        npgsqlOptions.EnableRetryOnFailure(5, TimeSpan.FromSeconds(30), null);
        npgsqlOptions.CommandTimeout(60);
    }));

builder.Services.AddHealthChecks()
    .AddDbContextCheck<TransactionDbContext>();

var host = builder.Build();

// Применяем миграции при запуске
using (var scope = host.Services.CreateScope())
{
    var contextFactory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<TransactionDbContext>>();
    await using var dbContext = await contextFactory.CreateDbContextAsync();

    try
    {
        // Применяет все ожидающие миграции
        await dbContext.Database.MigrateAsync();
        Console.WriteLine("✅ Database migrated successfully");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"⚠️ Migration warning: {ex.Message}");
        // Если миграции нет, создаем базу
        await dbContext.Database.EnsureCreatedAsync();
        Console.WriteLine("✅ Database ensured");
    }
}

await host.RunAsync();
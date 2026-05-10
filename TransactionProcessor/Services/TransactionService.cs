using Microsoft.EntityFrameworkCore;
using TransactionProcessor.Models;

namespace TransactionProcessor.Services;

public class TransactionService
{
    private readonly ILogger<TransactionService> _logger;
    private readonly IDbContextFactory<TransactionDbContext> _dbContextFactory;

    public TransactionService(
        ILogger<TransactionService> logger,
        IDbContextFactory<TransactionDbContext> dbContextFactory)
    {
        _logger = logger;
        _dbContextFactory = dbContextFactory;
    }

    public async Task<bool> ProcessTransactionEventAsync(KafkaEvent kafkaEvent)
    {
        await using var context = await _dbContextFactory.CreateDbContextAsync();

        // Используем стратегию повторных попыток для транзакций
        var strategy = context.Database.CreateExecutionStrategy();

        await strategy.ExecuteAsync(async () =>
        {
            // ИСПРАВЛЕНО: переименовали dbTransaction, чтобы избежать конфликта
            await using var dbTransaction = await context.Database.BeginTransactionAsync();

            try
            {
                if (kafkaEvent.EventType == "TRANSACTION_START")
                {
                    await HandleTransactionStartAsync(context, kafkaEvent);
                }
                else if (kafkaEvent.EventType == "TRANSACTION_STOP")
                {
                    await HandleTransactionStopAsync(context, kafkaEvent);
                }
                else
                {
                    _logger.LogWarning($"Unknown event type: {kafkaEvent.EventType}");
                }

                await dbTransaction.CommitAsync();
                _logger.LogInformation($"✅ Successfully processed {kafkaEvent.EventType} for {kafkaEvent.TransactionId}");

                return true;
            }
            catch (Exception ex)
            {
                await dbTransaction.RollbackAsync();
                _logger.LogError(ex, $"❌ Failed to process event for transaction {kafkaEvent.TransactionId}");
                throw;                
            }
        });

        return false;
    }

    private async Task HandleTransactionStartAsync(TransactionDbContext context, KafkaEvent kafkaEvent)
    {
        // ИСПРАВЛЕНО: переименовали existingTransaction в existingTrans
        var existingTrans = await context.Transactions
            .FirstOrDefaultAsync(t => t.TransactionId == kafkaEvent.TransactionId);

        if (existingTrans == null)
        {
            // Создаем новую транзакцию - переименовали в newTransaction
            var newTransaction = new Transaction
            {
                TransactionId = kafkaEvent.TransactionId,
                Status = "STARTED",
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow
            };

            await context.Transactions.AddAsync(newTransaction);

            // Добавляем событие
            var eventEntity = new TransactionEvent
            {
                TransactionId = kafkaEvent.TransactionId,
                EventType = kafkaEvent.EventType,
                EventTime = DateTime.UtcNow
            };

            await context.TransactionEvents.AddAsync(eventEntity);
            await context.SaveChangesAsync();
        }
        else
        {
            _logger.LogWarning($"Transaction {kafkaEvent.TransactionId} already exists, ignoring START event");
        }
    }

    private async Task HandleTransactionStopAsync(TransactionDbContext context, KafkaEvent kafkaEvent)
    {
        // Находим транзакцию - переименовали в existingTransaction
        var existingTransaction = await context.Transactions
            .FirstOrDefaultAsync(t => t.TransactionId == kafkaEvent.TransactionId);

        if (existingTransaction != null)
        {
            // Обновляем статус
            existingTransaction.Status = "COMPLETED";
            existingTransaction.UpdatedAt = DateTime.UtcNow;

            // Добавляем событие
            var eventEntity = new TransactionEvent
            {
                TransactionId = kafkaEvent.TransactionId,
                EventType = kafkaEvent.EventType,
                EventTime = DateTime.UtcNow
            };

            await context.TransactionEvents.AddAsync(eventEntity);
            await context.SaveChangesAsync();
        }
        else
        {
            _logger.LogWarning($"Transaction {kafkaEvent.TransactionId} not found for STOP event");

            // Создаем транзакцию в статусе ERROR - переименовали в errorTransaction
            var errorTransaction = new Transaction
            {
                TransactionId = kafkaEvent.TransactionId,
                Status = "ERROR",
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow
            };

            await context.Transactions.AddAsync(errorTransaction);

            var eventEntity = new TransactionEvent
            {
                TransactionId = kafkaEvent.TransactionId,
                EventType = kafkaEvent.EventType,
                EventTime = DateTime.UtcNow
            };

            await context.TransactionEvents.AddAsync(eventEntity);
            await context.SaveChangesAsync();
        }
    }

    public async Task<TransactionStatistics> GetStatisticsAsync()
    {
        await using var context = await _dbContextFactory.CreateDbContextAsync();

        var stats = new TransactionStatistics
        {
            TotalTransactions = await context.Transactions.CountAsync(),
            StartedTransactions = await context.Transactions.CountAsync(t => t.Status == "STARTED"),
            CompletedTransactions = await context.Transactions.CountAsync(t => t.Status == "COMPLETED"),
            ErrorTransactions = await context.Transactions.CountAsync(t => t.Status == "ERROR"),
            TotalEvents = await context.TransactionEvents.CountAsync()
        };

        return stats;
    }
}
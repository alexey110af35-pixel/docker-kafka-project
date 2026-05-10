using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Confluent.Kafka;
using System.Text.Json;
using System;
using System.Threading;
using System.Threading.Tasks;
using TransactionProcessor.Models;

namespace TransactionProcessor.Services
{
    // Убираем дублирование - используем тот же класс, что и в TransactionService
    // Если класс уже определен в TransactionService, удалите его отсюда

    public class KafkaConsumerService : BackgroundService
    {
        private readonly ILogger<KafkaConsumerService> _logger;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IConfiguration _configuration;
        private IConsumer<string, string>? _consumer;
        private readonly string _topic;
        private readonly string _groupId;

        public KafkaConsumerService(
            ILogger<KafkaConsumerService> logger,
            IServiceScopeFactory scopeFactory,
            IConfiguration configuration)
        {
            _logger = logger;
            _scopeFactory = scopeFactory;
            _configuration = configuration;
            _topic = _configuration["Kafka:Topic"] ?? "transactions";
            _groupId = _configuration["Kafka:GroupId"] ?? "transaction-processor-group";
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() => StartConsumer(stoppingToken), stoppingToken);
        }

        private void StartConsumer(CancellationToken stoppingToken)
        {
            var bootstrapServers = _configuration["Kafka:BootstrapServers"] ?? "localhost:9092";

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                AutoCommitIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                MaxPollIntervalMs = 300000,
                StatisticsIntervalMs = 60000
            };

            _consumer = new ConsumerBuilder<string, string>(config)
                .SetErrorHandler((_, e) => _logger.LogError($"Kafka error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => _logger.LogDebug($"Kafka stats: {json}"))
                .Build();

            try
            {
                _consumer.Subscribe(_topic);
                _logger.LogInformation($"📡 Subscribed to topic: {_topic}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Failed to subscribe to topic {_topic}");
                return;
            }

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = _consumer.Consume(stoppingToken);

                        if (consumeResult?.Message?.Value != null)
                        {
                            _logger.LogInformation($"📨 Received message from partition {consumeResult.Partition}");

                            // Десериализуем в KafkaEvent
                            var kafkaEvent = JsonSerializer.Deserialize<KafkaEvent>(consumeResult.Message.Value);
                            if (kafkaEvent != null)
                            {
                                _ = Task.Run(async () =>
                                {
                                    using var scope = _scopeFactory.CreateScope();
                                    var transactionService = scope.ServiceProvider.GetRequiredService<TransactionService>();
                                    await transactionService.ProcessTransactionEventAsync(kafkaEvent);
                                }, stoppingToken);
                            }
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, $"Consume error: {ex.Error.Reason}");
                        Thread.Sleep(1000);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Unexpected error while consuming message");
                        Thread.Sleep(1000);
                    }
                }
            }
            finally
            {
                _consumer?.Close();
                _consumer?.Dispose();
            }
        }

        public override async Task StopAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Stopping Kafka consumer...");
            _consumer?.Close();
            await base.StopAsync(stoppingToken);
        }
    }
}
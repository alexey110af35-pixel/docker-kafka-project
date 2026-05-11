using Confluent.Kafka;
using System.Text.Json;
using TransactionProcessor.Models;

namespace TransactionProcessor.Services
{
    public class RetryTopicConsumer : BackgroundService
    {
        private readonly IConsumer<string, string> _consumer;
        private readonly IProducer<string, string> _producer;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<RetryTopicConsumer> _logger;
        private readonly string _topic;

        public RetryTopicConsumer(
            IConfiguration config,
            IServiceScopeFactory scopeFactory,
            ILogger<RetryTopicConsumer> logger)
        {
            _topic = config["Kafka:Topic"] ?? "transactions-retry";

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = config["Kafka:BootstrapServers"],
                GroupId = "transaction-retry-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = config["Kafka:BootstrapServers"]
            };

            _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
            _producer = new ProducerBuilder<string, string>(producerConfig).Build();
            _scopeFactory = scopeFactory;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                // Подписываемся на retry-топик
                _consumer.Subscribe(_topic);
                _logger.LogInformation($"📡 Subscribed to topic: {_topic}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Failed to subscribe to topic {_topic}");
                return;
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(stoppingToken);

                    if ((consumeResult?.Message?.Value) == null)
                        continue;

                    // Проверяем, наступило ли время обработки
                    if (consumeResult.Message.Timestamp.UnixTimestampMs > DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
                    {
                        // Ещё не время — возвращаем в топик с той же задержкой
                        await _producer.ProduceAsync(_topic, new Message<string, string>
                        {
                            Key = consumeResult.Message.Key,
                            Value = consumeResult.Message.Value,
                            Timestamp = consumeResult.Message.Timestamp
                        });
                        _consumer.Commit(consumeResult);
                        continue;
                    }

                    // Время пришло — обрабатываем

                    _logger.LogInformation($"📨 Received message from partition {consumeResult.Partition}");

                    // Десериализуем в KafkaEvent
                    var kafkaEvent = JsonSerializer.Deserialize<KafkaEvent>(consumeResult.Message.Value);

                    if (kafkaEvent == null)
                        continue;

                    using var scope = _scopeFactory.CreateScope();
                    var transactionService = scope.ServiceProvider.GetRequiredService<TransactionService>();

                    var success = await transactionService.ProcessTransactionEventAsync(kafkaEvent);

                    if (success)
                    {
                        _consumer.Commit(consumeResult);
                        _logger.LogInformation("Retry transaction processed successfully");
                    }
                    else
                    {
                        // Вторая ошибка — отправляем в DLQ
                        await SendToDeadLetterQueue(consumeResult);
                        _consumer.Commit(consumeResult);
                        _logger.LogWarning("Transaction moved to DLQ after retry failure");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in retry consumer");
                    await Task.Delay(5000, stoppingToken);
                }
            }
        }

        private async Task SendToDeadLetterQueue(ConsumeResult<string, string> result)
        {
            var dlqMessage = new
            {
                OriginalMessage = result.Message.Value,
                OriginalTopic = result.Topic,
                OriginalPartition = result.Partition.Value,
                OriginalOffset = result.Offset.Value,
                FailedAt = DateTime.UtcNow,
                FailureReason = "Failed after retry"
            };

            await _producer.ProduceAsync("transactions-dlq", new Message<string, string>
            {
                Key = result.Message.Key,
                Value = JsonSerializer.Serialize(dlqMessage)
            });
        }

        public override void Dispose()
        {
            _consumer.Close();
            _consumer.Dispose();
            _producer.Dispose();
            base.Dispose();
        }
    }
}

using Confluent.Kafka;
using System.Text.Json;
using TransactionProcessor.Models;
using static Confluent.Kafka.ConfigPropertyNames;

namespace TransactionProcessor.Services
{
	public class KafkaConsumerService : BackgroundService
	{
		private readonly ILogger<KafkaConsumerService> _logger;
		private readonly IServiceScopeFactory _scopeFactory;
		private readonly IConsumer<string, string> _consumer;
		private readonly IProducer<string, string> _dlqProducer;
		private readonly Dictionary<string, int> _retryCount = new();
		private readonly string _topic;
		private readonly string _retryTopic;
		private readonly string _groupId;

		public KafkaConsumerService(
			ILogger<KafkaConsumerService> logger,
			IServiceScopeFactory scopeFactory,
			IConfiguration config)
		{
			_logger = logger;
			_scopeFactory = scopeFactory;
			_topic = config["Kafka:Topic"] ?? "transactions";
			_retryTopic = config["Kafka:Topic"] ?? "transactions-retry";
			_groupId = config["Kafka:GroupId"] ?? "transaction-processor-group";

			var consumerConfig = new ConsumerConfig
			{
				BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:9092",
				GroupId = _groupId,
				AutoOffsetReset = AutoOffsetReset.Earliest,
				EnableAutoCommit = false,
				AutoCommitIntervalMs = 5000,
				SessionTimeoutMs = 6000,
				MaxPollIntervalMs = 300000,
				StatisticsIntervalMs = 60000
			};

			var producerConfig = new ProducerConfig
			{
				BootstrapServers = config["Kafka:BootstrapServers"]
			};

			_consumer = new ConsumerBuilder<string, string>(consumerConfig)
				.SetErrorHandler((_, e) => _logger.LogError($"Kafka error: {e.Reason}"))
				.SetStatisticsHandler((_, json) => _logger.LogDebug($"Kafka stats: {json}"))
				.Build();

			_dlqProducer = new ProducerBuilder<string, string>(producerConfig).Build();
		}

		protected override async Task ExecuteAsync(CancellationToken stoppingToken)
		{
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

						if ((consumeResult?.Message?.Value) == null)
							continue;

						_logger.LogInformation($"📨 Received message from partition {consumeResult.Partition}");

						// Десериализуем в KafkaEvent
						var kafkaEvent = JsonSerializer.Deserialize<KafkaEvent>(consumeResult.Message.Value);

						if (kafkaEvent == null)
							continue;

						using var scope = _scopeFactory.CreateScope();
						var transactionService = scope.ServiceProvider.GetRequiredService<TransactionService>();

						bool success = false;

						try
						{
							var succcess = await transactionService.ProcessTransactionEventAsync(kafkaEvent);
						}
						catch (Exception ex)
						{
							_logger.LogError(ex, "Error processing transaction");
							success = false;
						}

						if (success)
						{
							_consumer.Commit(consumeResult);
							_logger.LogInformation("Transaction processed and committed");
							_retryCount.Remove(GetMessageKey(consumeResult));
						}
						else
						{
							string key = GetMessageKey(consumeResult);
							int attempts = _retryCount.GetValueOrDefault(key, 0);

							if (attempts >= 2) // 3 попытки всего
							{
								await SendToDeadLetterQueue(consumeResult, "Max retry attempts exceeded");
								_consumer.Commit(consumeResult);
								_retryCount.Remove(key);
							}
							else
							{
								_retryCount[key] = attempts + 1;
								// Отправляем в retry-топик с задержкой 5 секунд
								await SendToRetryTopic(consumeResult, delaySeconds: 5);
								_consumer.Commit(consumeResult); // Коммитим оригинал
								_logger.LogWarning("Message sent to retry topic, will be processed in 5 seconds");
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

		// Метод для отправки в DLQ
		private async Task SendToDeadLetterQueue(ConsumeResult<string, string> result, string reason)
		{
			var dlqMessage = new
			{
				OriginalMessage = result.Message.Value,
				OriginalTopic = result.Topic,
				OriginalPartition = result.Partition.Value,
				OriginalOffset = result.Offset.Value,
				FailedAt = DateTime.UtcNow,
				FailureReason = reason
			};

			await _dlqProducer.ProduceAsync("transactions-dlq", new Message<string, string>
			{
				Key = result.Message.Key,
				Value = JsonSerializer.Serialize(dlqMessage)
			});
			_logger.LogWarning("💀 Message sent to DLQ: {Reason}", reason);
		}


		private string GetMessageKey(ConsumeResult<string, string> result)
		{
			return $"{result.Topic}:{result.Partition}:{result.Offset}";
		}

		private async Task SendToRetryTopic(ConsumeResult<string, string> result, int delaySeconds)
		{
			var futureTimestamp = new Timestamp(DateTime.UtcNow.AddSeconds(delaySeconds));

			await _dlqProducer.ProduceAsync(_retryTopic, new Message<string, string>
			{
				Key = result.Message.Key,
				Value = result.Message.Value,
				Timestamp = futureTimestamp
			});
		}

		public override async Task StopAsync(CancellationToken stoppingToken)
		{
			_logger.LogInformation("Stopping Kafka consumer...");
			_consumer?.Close();
			await base.StopAsync(stoppingToken);
		}
	}
}
using Confluent.Kafka;
using System.Text.Json;
using TransactionAPI.Models;

namespace TransactionAPI.Services;

public class KafkaProducerService
{
    private readonly ILogger<KafkaProducerService> _logger;
    private readonly IConfiguration _configuration;
    private IProducer<string, string>? _producer;
    private string _topic = string.Empty;
    private bool _isInitialized = false;

    public KafkaProducerService(ILogger<KafkaProducerService> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        Initialize();
    }

    private void Initialize()
    {
        try
        {
            var bootstrapServers = _configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
            _topic = _configuration["Kafka:Topic"] ?? "transactions";

            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                ClientId = "transaction-api",
                Acks = Acks.All,
                EnableIdempotence = true,
                MessageTimeoutMs = 5000,
                SocketTimeoutMs = 5000
            };

            _producer = new ProducerBuilder<string, string>(config).Build();
            _isInitialized = true;
            _logger.LogInformation($"✅ Kafka producer initialized. Bootstrap servers: {bootstrapServers}, Topic: {_topic}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "❌ Failed to initialize Kafka producer");
        }
    }

    public async Task<bool> SendTransactionEventAsync(TransactionEventForKafka eventData)
    {
        if (!_isInitialized || _producer == null)
        {
            _logger.LogError("Producer not initialized");
            return false;
        }

        try
        {
            var json = JsonSerializer.Serialize(eventData);
            var message = new Message<string, string>
            {
                Key = eventData.TransactionId,
                Value = json
            };

            var result = await _producer.ProduceAsync(_topic, message);
            _logger.LogInformation($"✅ Message sent to partition {result.Partition}, offset {result.Offset}");
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "❌ Failed to send message to Kafka");
            return false;
        }
    }
}
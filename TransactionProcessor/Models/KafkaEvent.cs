namespace TransactionProcessor.Models;

public class KafkaEvent
{
    public string EventType { get; set; } = string.Empty;
    public string TransactionId { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
    public Dictionary<string, object> Data { get; set; } = new();
}
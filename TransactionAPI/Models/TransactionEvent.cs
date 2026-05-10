namespace TransactionAPI.Models;

// Добавляем public - сейчас это самый важный момент
public class TransactionEvent
{
    public string EventType { get; set; } = string.Empty;
    public string TransactionId { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
    public Dictionary<string, object> Data { get; set; } = new();
}
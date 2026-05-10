namespace TransactionProcessor.Models;

public class TransactionStatistics
{
    public int TotalTransactions { get; set; }
    public int StartedTransactions { get; set; }
    public int CompletedTransactions { get; set; }
    public int ErrorTransactions { get; set; }
    public int TotalEvents { get; set; }
}
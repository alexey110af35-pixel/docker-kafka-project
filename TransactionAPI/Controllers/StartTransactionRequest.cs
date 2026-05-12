namespace TransactionAPI.Controllers;

public class StartTransactionRequest
{
	public string? TransactionId { get; set; }
	public Dictionary<string, object>? Data { get; set; }
}
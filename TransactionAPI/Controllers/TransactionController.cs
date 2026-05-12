using Microsoft.AspNetCore.Mvc;
using TransactionAPI.Models;
using TransactionAPI.Services;

namespace TransactionAPI.Controllers;

[ApiController]
[Route("api/[controller]")]
public class TransactionController : ControllerBase
{
	private readonly KafkaProducerService _kafkaProducer;
	private readonly ILogger<TransactionController> _logger;

	public TransactionController(KafkaProducerService kafkaProducer, ILogger<TransactionController> logger)
	{
		_kafkaProducer = kafkaProducer;
		_logger = logger;
	}

	[HttpPost("start")]
	public async Task<IActionResult> StartTransaction([FromBody] StartTransactionRequest request)
	{
		try
		{
			var transactionId = request.TransactionId ?? Guid.NewGuid().ToString();

			// Используем правильную модель
			var eventData = new TransactionEventForKafka
			{
				EventType = "TRANSACTION_START",
				TransactionId = transactionId,
				Timestamp = DateTime.UtcNow,
				Data = request.Data ?? new Dictionary<string, object>()
			};

			var success = await _kafkaProducer.SendTransactionEventAsync(eventData);

			if (success)
			{
				return Accepted(new
				{
					status = "success",
					message = "Transaction started",
					transactionId
				});
			}

			return StatusCode(500, new { status = "error", message = "Failed to send event to Kafka" });
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Error starting transaction");
			return StatusCode(500, new { status = "error", message = ex.Message });
		}
	}

	[HttpPost("stop/{transactionId}")]
	public async Task<IActionResult> StopTransaction(string transactionId, [FromBody] Dictionary<string, object>? data = null)
	{
		try
		{
			// Используем правильную модель
			var eventData = new TransactionEventForKafka
			{
				EventType = "TRANSACTION_STOP",
				TransactionId = transactionId,
				Timestamp = DateTime.UtcNow,
				Data = data ?? new Dictionary<string, object>()
			};

			var success = await _kafkaProducer.SendTransactionEventAsync(eventData);

			if (success)
			{
				return Accepted(new
				{
					status = "success",
					message = "Transaction stopped",
					transactionId
				});
			}

			return StatusCode(500, new { status = "error", message = "Failed to send event to Kafka" });
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Error stopping transaction");
			return StatusCode(500, new { status = "error", message = ex.Message });
		}
	}
}
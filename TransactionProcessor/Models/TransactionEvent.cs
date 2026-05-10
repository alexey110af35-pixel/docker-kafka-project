using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TransactionProcessor.Models;

[Table("transaction_events")]
public class TransactionEvent
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    [Column("id")]
    public int Id { get; set; }

    [Column("transaction_id")]
    public string TransactionId { get; set; } = string.Empty;

    [Column("event_type")]
    public string EventType { get; set; } = string.Empty;

    [Column("event_time")]
    public DateTime EventTime { get; set; } = DateTime.UtcNow;

    [ForeignKey("TransactionId")]
    public virtual Transaction? Transaction { get; set; }
}
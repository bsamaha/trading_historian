namespace KafkaToInfluxDB.Models;

public class CandleData
{
    public long Start { get; set; }
    public decimal High { get; set; }
    public decimal Low { get; set; }
    public decimal Open { get; set; }
    public decimal Close { get; set; }
    public decimal Volume { get; set; }
    public string? ProductId { get; set; }
}
using Newtonsoft.Json.Linq;
using System;
using KafkaToInfluxDB.Exceptions;

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

    public static CandleData ParseCandleData(string message)
    {
        try
        {
            var jObject = JObject.Parse(message);
            return new CandleData
            {
                Start = jObject["start"]?.Value<long>() ?? 0,
                High = jObject["high"]?.Value<decimal>() ?? 0m,
                Low = jObject["low"]?.Value<decimal>() ?? 0m,
                Open = jObject["open"]?.Value<decimal>() ?? 0m,
                Close = jObject["close"]?.Value<decimal>() ?? 0m,
                Volume = jObject["volume"]?.Value<decimal>() ?? 0m,
                ProductId = jObject["product_id"]?.Value<string>()
            };
        }
        catch (Exception ex)
        {
            throw new KafkaToInfluxDBException("Error parsing candle data", ex);
        }
    }
}
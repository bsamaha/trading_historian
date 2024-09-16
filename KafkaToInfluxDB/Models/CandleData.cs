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

    public const string MeasurementName = "candle_data";

    public Dictionary<string, object> ToFields()
    {
        return new Dictionary<string, object>
        {
            { nameof(Open), Open },
            { nameof(High), High },
            { nameof(Low), Low },
            { nameof(Close), Close },
            { nameof(Volume), Volume },
            { nameof(Start), Start }
        };
    }

    public Dictionary<string, string> ToTags()
    {
        return new Dictionary<string, string>
        {
            { "product_id", ProductId ?? "unknown" }
        };
    }

    public static CandleData ParseCandleData(string message)
    {
        try
        {
            var jObject = JObject.Parse(message);
            var candle = jObject["events"]?[0]?["candles"]?[0] as JObject;
            if (candle == null)
            {
                throw new KafkaToInfluxDBException("Invalid candle data structure");
            }

            return new CandleData
            {
                Start = candle["start"]?.Value<long>() ?? 0,
                High = candle["high"]?.Value<decimal>() ?? 0m,
                Low = candle["low"]?.Value<decimal>() ?? 0m,
                Open = candle["open"]?.Value<decimal>() ?? 0m,
                Close = candle["close"]?.Value<decimal>() ?? 0m,
                Volume = candle["volume"]?.Value<decimal>() ?? 0m,
                ProductId = candle["product_id"]?.Value<string>()
            };
        }
        catch (Exception ex)
        {
            throw new KafkaToInfluxDBException("Error parsing candle data", ex);
        }
    }
}
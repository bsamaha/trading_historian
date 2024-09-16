using KafkaToInfluxDB.Models;
using Newtonsoft.Json;
using System;

namespace KafkaToInfluxDB.Services
{
    public class CandleDataParser : ICandleDataParser
    {
        public CandleData Parse(string message)
        {
            if (string.IsNullOrWhiteSpace(message))
            {
                throw new ArgumentException("Message cannot be null or whitespace.", nameof(message));
            }

            var candleData = JsonConvert.DeserializeObject<CandleData>(message);
            if (candleData == null)
            {
                throw new InvalidOperationException("Failed to deserialize the message into CandleData.");
            }

            return candleData;
        }
    }
}
using KafkaToInfluxDB.Models;
using System;

namespace KafkaToInfluxDB.Services
{
    public interface ICandleDataParser
    {
        /// <summary>
        /// Parses a string message into a CandleData object.
        /// </summary>
        /// <param name="message">The message to parse.</param>
        /// <returns>A CandleData object.</returns>
        /// <exception cref="ArgumentException">Thrown when the message is null or whitespace.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the message cannot be deserialized into CandleData.</exception>
        CandleData Parse(string message);
    }
}
using System;

namespace KafkaToInfluxDB.Exceptions
{
    public class KafkaToInfluxDBException : Exception
    {
        public KafkaToInfluxDBException(string message) : base(message) { }
        public KafkaToInfluxDBException(string message, Exception innerException) : base(message, innerException) { }
    }

    public class KafkaConsumerException : KafkaToInfluxDBException
    {
        public KafkaConsumerException(string message, Exception innerException) : base(message, innerException) { }
    }

    public class InfluxDBWriteException : KafkaToInfluxDBException
    {
        public InfluxDBWriteException(string message, Exception innerException) : base(message, innerException) { }
    }
}
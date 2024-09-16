using Microsoft.Extensions.Logging;
using Confluent.Kafka;

namespace KafkaToInfluxDB.Services;

public class AppConfig
{
    public KafkaConfig Kafka { get; set; } = new KafkaConfig();
    public BatchConfig Batch { get; set; } = new BatchConfig();
    public InfluxDBConfig InfluxDB { get; set; } = new InfluxDBConfig();
    public LoggingConfig Logging { get; set; } = new LoggingConfig();

    public class KafkaConfig
    {
        public string BootstrapServers { get; set; } = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "";
        public string Username { get; set; } = Environment.GetEnvironmentVariable("KAFKA_SASL_USERNAME") ?? "";
        public string Password { get; set; } = Environment.GetEnvironmentVariable("KAFKA_PASSWORD") ?? "";
        public string SecurityProtocol { get; set; } = Environment.GetEnvironmentVariable("KAFKA_SECURITY_PROTOCOL") ?? "";
        public string SaslMechanism { get; set; } = Environment.GetEnvironmentVariable("KAFKA_SASL_MECHANISM") ?? "";
        public string GroupId { get; set; } = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") ?? "";
        public string Topic { get; set; } = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "";
        public int MaxParallelism { get; set; } = int.Parse(Environment.GetEnvironmentVariable("KAFKA_MAX_PARALLELISM") ?? "10");
    }

    public class BatchConfig
    {
        public int Size { get; set; } = int.Parse(Environment.GetEnvironmentVariable("BATCH_SIZE") ?? "100");
        public int FlushIntervalSeconds { get; set; } = int.Parse(Environment.GetEnvironmentVariable("BATCH_FLUSH_INTERVAL_SECONDS") ?? "5");
        public int MaxQueueSize { get; set; } = int.Parse(Environment.GetEnvironmentVariable("BATCH_MAX_QUEUE_SIZE") ?? "10000");
    }

    public class InfluxDBConfig
    {
        public string Url { get; set; } = Environment.GetEnvironmentVariable("INFLUXDB_URL") ?? "";
        public string Token { get; set; } = Environment.GetEnvironmentVariable("INFLUXDB_TOKEN") ?? "";
        public string Bucket { get; set; } = Environment.GetEnvironmentVariable("INFLUXDB_BUCKET") ?? "";
        public string Org { get; set; } = Environment.GetEnvironmentVariable("INFLUXDB_ORG") ?? "";
        public int MaxRetries { get; set; } = int.Parse(Environment.GetEnvironmentVariable("INFLUXDB_MAX_RETRIES") ?? "3");
        public int BaseDelayMs { get; set; } = int.Parse(Environment.GetEnvironmentVariable("INFLUXDB_BASE_DELAY_MS") ?? "1000");
    }

    public class LoggingConfig
    {
        public LogLevel MinimumLevel { get; set; } = Enum.TryParse<LogLevel>(Environment.GetEnvironmentVariable("LOG_LEVEL"), true, out var logLevel) 
            ? logLevel 
            : LogLevel.Information;
    }
}
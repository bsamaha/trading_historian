using Microsoft.Extensions.Logging;
using Confluent.Kafka;

namespace KafkaToInfluxDB;

public class AppConfig
{
    public KafkaConfig Kafka { get; set; } = new KafkaConfig();
    public InfluxDBConfig InfluxDB { get; set; } = new InfluxDBConfig();

    public class KafkaConfig
    {
        public string? BootstrapServers { get; set; }
        public string? GroupId { get; set; }
        public string? Topic { get; set; }
        public string? Username { get; set; }
        public string? Password { get; set; }
        public string? SecurityProtocol { get; set; }
        public string? SaslMechanism { get; set; }
    }

    public class InfluxDBConfig
    {
        public string Url { get; set; } = "http://influxdb-influxdb2.default.svc.cluster.local:80";
        public string Bucket { get; set; } = "default";
        public string Token { get; set; } = "f09f13b863c83737cb5317220546afee3cf273fd042152d123c971342253a59e";
        public string Org { get; set; } = "my-org";
    }

    public static AppConfig LoadConfig(ILogger<AppConfig> logger)
    {
        logger.LogInformation("Loading application configuration from environment variables");
        var config = new AppConfig
        {
            Kafka = new KafkaConfig
            {
                BootstrapServers = GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS", logger),
                GroupId = GetEnvironmentVariable("KAFKA_GROUP_ID", logger),
                Topic = GetEnvironmentVariable("KAFKA_TOPIC", logger),
                Username = GetEnvironmentVariable("KAFKA_SASL_USERNAME", logger),
                Password = GetEnvironmentVariable("KAFKA_PASSWORD", logger),
                SecurityProtocol = GetEnvironmentVariable("KAFKA_SECURITY_PROTOCOL", logger),
                SaslMechanism = GetEnvironmentVariable("KAFKA_SASL_MECHANISM", logger)
            },
            InfluxDB = new InfluxDBConfig
            {
                Url = GetEnvironmentVariable("INFLUXDB_URL", logger),
                Bucket = GetEnvironmentVariable("INFLUXDB_BUCKET", logger),
                Token = GetEnvironmentVariable("INFLUXDB_TOKEN", logger),
                Org = GetEnvironmentVariable("INFLUXDB_ORG", logger)
            }
        };

        ValidateConfiguration(config, logger);
        ValidateKafkaSecuritySettings(config.Kafka, logger);

        logger.LogInformation("Configuration loaded successfully");
        return config;
    }

    private static string GetEnvironmentVariable(string variableName, ILogger logger)
    {
        var value = Environment.GetEnvironmentVariable(variableName);
        if (string.IsNullOrEmpty(value))
        {
            logger.LogError("Environment variable {VariableName} is not set or empty", variableName);
            throw new InvalidOperationException($"Environment variable {variableName} is not set or empty");
        }
        logger.LogDebug("Environment variable {VariableName} loaded successfully", variableName);
        return value;
    }

    private static void ValidateConfiguration(AppConfig config, ILogger logger)
    {
        if (string.IsNullOrEmpty(config.Kafka.BootstrapServers))
        {
            throw new InvalidOperationException("Kafka BootstrapServers is not configured");
        }

        if (string.IsNullOrEmpty(config.Kafka.Topic))
        {
            throw new InvalidOperationException("Kafka Topic is not configured");
        }

        if (string.IsNullOrEmpty(config.InfluxDB.Url))
        {
            throw new InvalidOperationException("InfluxDB Url is not configured");
        }

        if (string.IsNullOrEmpty(config.InfluxDB.Bucket))
        {
            throw new InvalidOperationException("InfluxDB Bucket is not configured");
        }

        logger.LogInformation("Configuration validation completed successfully");
    }

    private static void ValidateKafkaSecuritySettings(KafkaConfig kafkaConfig, ILogger logger)
    {
        if (!string.IsNullOrEmpty(kafkaConfig.SecurityProtocol))
        {
            if (!Enum.TryParse<SecurityProtocol>(kafkaConfig.SecurityProtocol, out _))
            {
                logger.LogWarning("Invalid Kafka SecurityProtocol: {SecurityProtocol}", kafkaConfig.SecurityProtocol);
            }
        }

        if (!string.IsNullOrEmpty(kafkaConfig.SaslMechanism))
        {
            if (!Enum.TryParse<SaslMechanism>(kafkaConfig.SaslMechanism, out _))
            {
                logger.LogWarning("Invalid Kafka SaslMechanism: {SaslMechanism}", kafkaConfig.SaslMechanism);
            }
        }
    }
}
using Microsoft.Extensions.Configuration;
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
        public string? Url { get; set; }
        public string? Bucket { get; set; }
        public string? Org { get; set; }
        public string? Token { get; set; }
    }

    public static AppConfig LoadConfig(IConfiguration configuration, ILogger<AppConfig> logger)
    {
        logger.LogInformation("Loading application configuration");
        var config = new AppConfig();

        try
        {
            configuration.Bind(config);
            logger.LogDebug("Configuration bound successfully");

            // Load sensitive data from environment variables
            config.Kafka.Password = GetEnvironmentVariable("KAFKA_PASSWORD", logger);
            config.Kafka.BootstrapServers = GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS", logger) ?? config.Kafka.BootstrapServers;
            config.Kafka.Username = GetEnvironmentVariable("KAFKA_SASL_USERNAME", logger) ?? config.Kafka.Username;
            config.Kafka.SecurityProtocol = GetEnvironmentVariable("KAFKA_SECURITY_PROTOCOL", logger) ?? config.Kafka.SecurityProtocol;
            config.Kafka.SaslMechanism = GetEnvironmentVariable("KAFKA_SASL_MECHANISM", logger) ?? config.Kafka.SaslMechanism;
            config.InfluxDB.Token = GetEnvironmentVariable("INFLUXDB_TOKEN", logger);
            config.InfluxDB.Url = GetEnvironmentVariable("INFLUXDB_URL", logger) ?? config.InfluxDB.Url;

            ValidateConfiguration(config, logger);
            ValidateKafkaSecuritySettings(config.Kafka, logger);

            logger.LogInformation("Configuration loaded successfully");
            return config;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error loading configuration");
            throw new InvalidOperationException("Failed to load application configuration", ex);
        }
    }

    private static string? GetEnvironmentVariable(string variableName, ILogger logger)
    {
        var value = Environment.GetEnvironmentVariable(variableName);
        if (string.IsNullOrEmpty(value))
        {
            logger.LogWarning("Environment variable {VariableName} is not set or empty", variableName);
        }
        else
        {
            logger.LogDebug("Environment variable {VariableName} loaded successfully", variableName);
        }
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

        if (string.IsNullOrEmpty(config.InfluxDB.Org))
        {
            throw new InvalidOperationException("InfluxDB Org is not configured");
        }

        logger.LogInformation("Configuration validation completed successfully");
    }

    private static void ValidateKafkaSecuritySettings(KafkaConfig kafkaConfig, ILogger logger)
    {
        if (!Enum.TryParse<SecurityProtocol>(kafkaConfig.SecurityProtocol, out _))
        {
            logger.LogWarning("Invalid Kafka SecurityProtocol: {SecurityProtocol}", kafkaConfig.SecurityProtocol);
        }

        if (!Enum.TryParse<SaslMechanism>(kafkaConfig.SaslMechanism, out _))
        {
            logger.LogWarning("Invalid Kafka SaslMechanism: {SaslMechanism}", kafkaConfig.SaslMechanism);
        }
    }
}
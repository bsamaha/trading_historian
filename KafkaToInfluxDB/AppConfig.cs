using Microsoft.Extensions.Configuration;

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
    }

    public class InfluxDBConfig
    {
        public string? Url { get; set; }
        public string? Bucket { get; set; }
        public string? Org { get; set; }
        public string? Token { get; set; }
    }

    public static AppConfig LoadConfig(IConfiguration configuration)
    {
        var config = new AppConfig();
        configuration.Bind(config);

        // Load sensitive data from environment variables
        config.Kafka.Password = Environment.GetEnvironmentVariable("KAFKA_PASSWORD");
        config.InfluxDB.Token = Environment.GetEnvironmentVariable("INFLUXDB_TOKEN");

        return config;
    }
}
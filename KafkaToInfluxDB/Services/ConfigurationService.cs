using Microsoft.Extensions.Configuration;

namespace KafkaToInfluxDB.Services;

public class ConfigurationService
{
    private readonly IConfiguration _configuration;

    public ConfigurationService(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public AppConfig GetAppConfig()
    {
        var config = new AppConfig();
        _configuration.Bind(config);

        // Load sensitive data from environment variables
        config.Kafka.Password = Environment.GetEnvironmentVariable("KAFKA_PASSWORD")!;
        config.InfluxDB.Token = Environment.GetEnvironmentVariable("INFLUXDB_TOKEN")!;

        return config;
    }
}
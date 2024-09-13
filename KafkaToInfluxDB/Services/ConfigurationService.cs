using Microsoft.Extensions.Logging;
using KafkaToInfluxDB.Services;

namespace KafkaToInfluxDB.Services;

public class ConfigurationService
{
    private readonly ILogger<AppConfig> _logger;

    public ConfigurationService(ILogger<AppConfig> logger)
    {
        _logger = logger;
    }

    public AppConfig GetAppConfig()
    {
        return AppConfig.LoadConfig(_logger);
    }
}
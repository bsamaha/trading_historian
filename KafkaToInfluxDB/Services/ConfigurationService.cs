using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace KafkaToInfluxDB.Services;

public class ConfigurationService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<AppConfig> _logger;

    public ConfigurationService(IConfiguration configuration, ILogger<AppConfig> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    public AppConfig GetAppConfig()
    {
        return AppConfig.LoadConfig(_configuration, _logger);
    }
}
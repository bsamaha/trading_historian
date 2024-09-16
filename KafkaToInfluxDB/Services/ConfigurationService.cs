using Microsoft.Extensions.Configuration;
using System;

namespace KafkaToInfluxDB.Services
{
    public class ConfigurationService
    {
        private readonly IConfiguration _configuration;

        public ConfigurationService(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public AppConfig GetAppConfig()
        {
            var appConfig = new AppConfig();
            _configuration.Bind(appConfig);
            return appConfig;
        }
    }
}

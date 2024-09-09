using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using KafkaToInfluxDB.Services;
using KafkaToInfluxDB.Exceptions;

namespace KafkaToInfluxDB;

public class Program
{
    public static async Task Main(string[] args)
    {
        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddSingleton<ConfigurationService>();
                    services.AddSingleton(sp => sp.GetRequiredService<ConfigurationService>().GetAppConfig());
                    services.AddSingleton<IInfluxDBService, InfluxDBService>();
                    services.AddHostedService<KafkaConsumerService>();
                })
                .Build();

            var influxDBService = host.Services.GetRequiredService<IInfluxDBService>();
            await influxDBService.EnsureBucketExistsAsync();

            await host.RunAsync();
        }
        catch (Exception ex)
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var logger = loggerFactory.CreateLogger<Program>();
            logger.LogCritical(ex, "An unhandled exception occurred during startup");
            throw;
        }
    }
}
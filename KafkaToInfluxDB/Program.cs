using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using KafkaToInfluxDB.Services;

namespace KafkaToInfluxDB;

public class Program
{
    public static async Task Main(string[] args)
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

        // Ensure InfluxDB bucket exists before starting the KafkaConsumerService
        var influxDBService = host.Services.GetRequiredService<IInfluxDBService>();
        await influxDBService.EnsureBucketExistsAsync();

        await host.RunAsync();
    }
}
using KafkaToInfluxDB.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaToInfluxDB;

public class Program
{
    public static async Task Main(string[] args)
    {
        try
        {
            var builder = WebApplication.CreateBuilder(args);

            // Add services to the container.
            builder.Services.AddSingleton<ConfigurationService>();
            builder.Services.AddSingleton(sp => sp.GetRequiredService<ConfigurationService>().GetAppConfig());
            builder.Services.AddSingleton<IInfluxDBService, InfluxDBService>();
            builder.Services.AddHostedService<KafkaConsumerService>();
            builder.Services.AddHostedService<DataGeneratorService>();

            var app = builder.Build();

            // Configure the HTTP request pipeline.
            app.UseRouting();

            app.MapGet("/health", async context =>
            {
                var influxDBService = context.RequestServices.GetRequiredService<IInfluxDBService>();
                var isHealthy = await influxDBService.CheckHealthAsync();
                context.Response.StatusCode = isHealthy ? 200 : 503;
                await context.Response.WriteAsync(isHealthy ? "Healthy" : "Unhealthy");
            });

            app.MapGet("/ready", async context =>
            {
                var influxDBService = context.RequestServices.GetRequiredService<IInfluxDBService>();
                var isReady = await influxDBService.CheckHealthAsync();
                context.Response.StatusCode = isReady ? 200 : 503;
                await context.Response.WriteAsync(isReady ? "Ready" : "Not Ready");
            });

            await app.RunAsync();
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
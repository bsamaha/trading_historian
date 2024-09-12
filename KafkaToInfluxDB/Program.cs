using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using KafkaToInfluxDB.Services;
using KafkaToInfluxDB.Exceptions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;

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

            var app = builder.Build();

            // Configure the HTTP request pipeline.
            app.UseRouting();

            app.MapGet("/health", async context =>
            {
                context.Response.StatusCode = 200;
                await context.Response.WriteAsync("Healthy");
            });

            app.MapGet("/ready", async context =>
            {
                context.Response.StatusCode = 200;
                await context.Response.WriteAsync("Ready");
            });

            app.MapGet("/influxdb-health", async context =>
            {
                var influxDBService = context.RequestServices.GetRequiredService<IInfluxDBService>();
                var isHealthy = await influxDBService.CheckHealthAsync();
                context.Response.StatusCode = isHealthy ? 200 : 500;
                await context.Response.WriteAsync(isHealthy ? "InfluxDB is healthy" : "InfluxDB is not healthy");
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
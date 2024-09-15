using KafkaToInfluxDB.HealthChecks;
using KafkaToInfluxDB.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;
using System;

namespace KafkaToInfluxDB;

public class Program
{
    public static void Main(string[] args)
    {
        CreateHostBuilder(args).Build().Run();
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder.UseKestrel(options =>
                {
                    options.ListenAnyIP(8080);
                });
            })
            .ConfigureServices((hostContext, services) =>
            {
                services.AddSingleton<ConfigurationService>();
                services.AddSingleton(sp => sp.GetRequiredService<ConfigurationService>().GetAppConfig());
                services.AddSingleton<IInfluxDBService, InfluxDBService>();
                services.AddSingleton<IConsumer<Ignore, string>>(sp =>
                {
                    var appConfig = sp.GetRequiredService<AppConfig>();
                    var config = new ConsumerConfig
                    {
                        BootstrapServers = appConfig.Kafka.BootstrapServers,
                        GroupId = appConfig.Kafka.GroupId,
                        AutoOffsetReset = AutoOffsetReset.Latest,
                        EnableAutoCommit = false
                    };

                    if (!string.IsNullOrEmpty(appConfig.Kafka.SecurityProtocol))
                    {
                        config.SecurityProtocol = Enum.Parse<SecurityProtocol>(appConfig.Kafka.SecurityProtocol);
                    }

                    if (!string.IsNullOrEmpty(appConfig.Kafka.SaslMechanism))
                    {
                        config.SaslMechanism = Enum.Parse<SaslMechanism>(appConfig.Kafka.SaslMechanism);
                    }

                    if (!string.IsNullOrEmpty(appConfig.Kafka.Username))
                    {
                        config.SaslUsername = appConfig.Kafka.Username;
                    }

                    if (!string.IsNullOrEmpty(appConfig.Kafka.Password))
                    {
                        config.SaslPassword = appConfig.Kafka.Password;
                    }

                    return new ConsumerBuilder<Ignore, string>(config).Build();
                });

                services.AddHostedService<KafkaConsumerService>();
                services.AddHostedService<DataGeneratorService>();
                services.AddHostedService<GracefulShutdownService>();

                services.AddLogging(builder =>
                {
                    builder.AddConfiguration(hostContext.Configuration.GetSection("Logging"));
                    builder.AddConsole();
                    builder.SetMinimumLevel(LogLevel.Information);
                });

                services.AddHealthChecks()
                    .AddCheck<KafkaHealthCheck>("kafka_health_check")
                    .AddCheck<InfluxDBHealthCheck>("influxdb_health_check");

                services.AddControllers();

                services.AddSingleton<IAdminClient>(sp =>
                {
                    var appConfig = sp.GetRequiredService<AppConfig>();
                    var config = new AdminClientConfig
                    {
                        BootstrapServers = appConfig.Kafka.BootstrapServers
                    };

                    // Add any additional configuration (like security settings) here
                    if (!string.IsNullOrEmpty(appConfig.Kafka.SecurityProtocol))
                    {
                        config.SecurityProtocol = Enum.Parse<SecurityProtocol>(appConfig.Kafka.SecurityProtocol);
                    }

                    if (!string.IsNullOrEmpty(appConfig.Kafka.SaslMechanism))
                    {
                        config.SaslMechanism = Enum.Parse<SaslMechanism>(appConfig.Kafka.SaslMechanism);
                    }

                    if (!string.IsNullOrEmpty(appConfig.Kafka.Username))
                    {
                        config.SaslUsername = appConfig.Kafka.Username;
                    }

                    if (!string.IsNullOrEmpty(appConfig.Kafka.Password))
                    {
                        config.SaslPassword = appConfig.Kafka.Password;
                    }

                    return new AdminClientBuilder(config).Build();
                });
            });
}

public class GracefulShutdownService : IHostedService
{
    private readonly IHostApplicationLifetime _appLifetime;
    private readonly ILogger<GracefulShutdownService> _logger;

    public GracefulShutdownService(
        IHostApplicationLifetime appLifetime,
        ILogger<GracefulShutdownService> logger)
    {
        _appLifetime = appLifetime;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _appLifetime.ApplicationStarted.Register(OnStarted);
        _appLifetime.ApplicationStopping.Register(OnStopping);
        _appLifetime.ApplicationStopped.Register(OnStopped);

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    private void OnStarted()
    {
        _logger.LogInformation("Application started. Press Ctrl+C to shut down.");
    }

    private void OnStopping()
    {
        _logger.LogInformation("Application is shutting down...");
        // Add a delay to allow in-flight operations to complete
        Task.Delay(TimeSpan.FromSeconds(5)).Wait();
        _logger.LogInformation("Shutdown delay completed.");
    }

    private void OnStopped()
    {
        _logger.LogInformation("Application has stopped.");
    }
}

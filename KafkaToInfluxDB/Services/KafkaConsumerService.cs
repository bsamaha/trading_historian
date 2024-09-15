using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using KafkaToInfluxDB.Models;
using KafkaToInfluxDB.Exceptions;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaToInfluxDB.Services;

public class KafkaConsumerService : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly IInfluxDBService _influxDbService;
    private readonly IConsumer<Ignore, string> _consumer;
    private readonly AppConfig _appConfig;
    private readonly CancellationTokenSource _cts = new CancellationTokenSource();

    public KafkaConsumerService(
        ILogger<KafkaConsumerService> logger,
        IInfluxDBService influxDbService,
        IConsumer<Ignore, string> consumer,
        AppConfig appConfig)
    {
        _logger = logger;
        _influxDbService = influxDbService;
        _consumer = consumer;
        _appConfig = appConfig;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("KafkaConsumerService is starting.");
        _consumer.Subscribe(_appConfig.Kafka.Topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1));
                    if (consumeResult != null)
                    {
                        _logger.LogInformation("Received message: {Message}", consumeResult.Message.Value);
                        var candleData = CandleData.ParseCandleData(consumeResult.Message.Value);
                        await _influxDbService.WriteDataAsync(
                            "candle_data",
                            new Dictionary<string, object>
                            {
                                { "open", candleData.Open },
                                { "high", candleData.High },
                                { "low", candleData.Low },
                                { "close", candleData.Close },
                                { "volume", candleData.Volume }
                            },
                            new Dictionary<string, string>
                            {
                                { "product_id", candleData.ProductId ?? "unknown" }
                            },
                            _cts.Token
                        );
                        _consumer.Commit(consumeResult);
                        _logger.LogInformation("Processed and wrote data to InfluxDB");
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Consume operation canceled, shutting down.");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing Kafka message");
                }
            }
        }
        finally
        {
            _logger.LogInformation("KafkaConsumerService is stopping. Closing consumer...");
            _consumer.Close();
        }

        _logger.LogInformation("KafkaConsumerService has stopped.");
    }

    public override async Task StopAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("KafkaConsumerService is stopping.");
        _cts.Cancel();
        await base.StopAsync(stoppingToken);
    }

    public override void Dispose()
    {
        _cts.Dispose();
        _consumer?.Dispose();
        base.Dispose();
    }
}
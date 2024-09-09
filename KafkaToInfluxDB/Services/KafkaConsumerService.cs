using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using KafkaToInfluxDB.Models;
using KafkaToInfluxDB.Exceptions;

namespace KafkaToInfluxDB.Services;

public class KafkaConsumerService : BackgroundService
{
    private readonly AppConfig _appConfig;
    private readonly IInfluxDBService _influxDBService;
    private readonly ILogger<KafkaConsumerService> _logger;

    public KafkaConsumerService(AppConfig appConfig, IInfluxDBService influxDBService, ILogger<KafkaConsumerService> logger)
    {
        _appConfig = appConfig;
        _influxDBService = influxDBService;
        _logger = logger;
    }

    public override void Dispose()
    {
        // Implement disposal logic here
        base.Dispose();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting KafkaConsumerService with configuration: {@KafkaConfig}", new
        {
            BootstrapServers = _appConfig.Kafka.BootstrapServers,
            GroupId = _appConfig.Kafka.GroupId,
            Topic = _appConfig.Kafka.Topic,
            Username = _appConfig.Kafka.Username,
            HasPassword = !string.IsNullOrEmpty(_appConfig.Kafka.Password)
        });

        var config = new ConsumerConfig
        {
            BootstrapServers = _appConfig.Kafka.BootstrapServers,
            GroupId = _appConfig.Kafka.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.ScramSha256,
            SaslUsername = _appConfig.Kafka.Username,
            SaslPassword = _appConfig.Kafka.Password
        };

        try
        {
            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe(_appConfig.Kafka.Topic);
            _logger.LogInformation("Successfully connected to Kafka and subscribed to topic: {Topic}", _appConfig.Kafka.Topic);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = consumer.Consume(stoppingToken);
                    if (consumeResult == null) continue;

                    var message = consumeResult.Message.Value;

                    _logger.LogDebug("Received message: {@Message}", message);

                    CandleData candleData;
                    try
                    {
                        candleData = ParseCandleData(message);
                    }
                    catch (ArgumentException ex)
                    {
                        _logger.LogWarning(ex, "Failed to parse candle data: {Message}", message);
                        continue;
                    }
                    await _influxDBService.WritePointAsync(candleData);

                    _logger.LogInformation("Written to InfluxDB: {ProductId} at {Start}", candleData.ProductId, candleData.Start);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Error consuming message");
                    throw new KafkaConsumerException("Error consuming message from Kafka", ex);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unexpected error");
                    throw new KafkaToInfluxDBException("Unexpected error in Kafka consumer", ex);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to connect to Kafka or subscribe to topic");
            throw;
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping KafkaConsumerService");
        await base.StopAsync(cancellationToken);
    }

    private static CandleData ParseCandleData(string message)
    {
        var jsonObject = JObject.Parse(message);
        var candles = jsonObject["events"]?[0]?["candles"]?[0];

        if (candles == null)
        {
            throw new ArgumentException("Invalid message format", nameof(message));
        }

        return new CandleData
        {
            Start = candles["start"]?.ToObject<long>() ?? throw new ArgumentException("Invalid start value"),
            High = candles["high"]?.ToObject<decimal>() ?? throw new ArgumentException("Invalid high value"),
            Low = candles["low"]?.ToObject<decimal>() ?? throw new ArgumentException("Invalid low value"),
            Open = candles["open"]?.ToObject<decimal>() ?? throw new ArgumentException("Invalid open value"),
            Close = candles["close"]?.ToObject<decimal>() ?? throw new ArgumentException("Invalid close value"),
            Volume = candles["volume"]?.ToObject<decimal>() ?? throw new ArgumentException("Invalid volume value"),
            ProductId = candles["product_id"]?.ToString() ?? throw new ArgumentException("Invalid product_id value")
        };
    }
}
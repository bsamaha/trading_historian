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
    private IConsumer<string, string>? _consumer;

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
            SaslPassword = _appConfig.Kafka.Password,
            EnableAutoCommit = false
        };

        try
        {
            using (_consumer = new ConsumerBuilder<string, string>(config).Build())
            {
                _consumer.Subscribe(_appConfig.Kafka.Topic);
                _logger.LogInformation("Successfully connected to Kafka and subscribed to topic: {Topic}", _appConfig.Kafka.Topic);

                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1));

                        if (consumeResult == null)
                        {
                            continue;
                        }

                        _logger.LogDebug("Message consumed successfully. Offset: {Offset}", consumeResult.Offset);

                        var message = consumeResult.Message.Value;
                        _logger.LogDebug("Received message: {@Message}", message);

                        CandleData candleData = ParseCandleData(message);
                        await _influxDBService.WritePointAsync(candleData);

                        _logger.LogInformation("Written to InfluxDB: {ProductId} at {Start}", candleData.ProductId, candleData.Start);

                        _consumer.Commit(consumeResult);
                        _logger.LogDebug("Committed offset: {Offset}", consumeResult.Offset);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Error consuming message");
                        await Task.Delay(5000, stoppingToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break; // Exit the loop when cancellation is requested
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "An unexpected error occurred while consuming messages");
                        await Task.Delay(5000, stoppingToken);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // This is expected during shutdown, so we can ignore it
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to connect to Kafka or subscribe to topic");
            throw;
        }
        finally
        {
            _logger.LogInformation("KafkaConsumerService is shutting down");
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping KafkaConsumerService");
        _consumer?.Close();
        _consumer?.Dispose();
        await base.StopAsync(cancellationToken);
        _logger.LogInformation("KafkaConsumerService has been stopped");
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
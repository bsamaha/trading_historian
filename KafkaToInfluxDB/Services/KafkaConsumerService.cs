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
    private readonly IConsumer<Ignore, string> _consumer;
    private readonly ICandleDataParser _parser;
    private readonly IDataWriter _dataWriter;
    private readonly AppConfig _appConfig;
    private readonly SemaphoreSlim _maxParallelism;

    public KafkaConsumerService(
        ILogger<KafkaConsumerService> logger,
        IConsumer<Ignore, string> consumer,
        ICandleDataParser parser,
        IDataWriter dataWriter,
        AppConfig appConfig)
    {
        _logger = logger;
        _consumer = consumer;
        _parser = parser;
        _dataWriter = dataWriter;
        _appConfig = appConfig;
        _maxParallelism = new SemaphoreSlim(_appConfig.Kafka.MaxParallelism);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("KafkaConsumerService is starting.");
        _consumer.Subscribe(_appConfig.Kafka.Topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1));
                if (consumeResult != null)
                {
                    await _maxParallelism.WaitAsync(stoppingToken);
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await ProcessMessageAsync(consumeResult, stoppingToken);
                        }
                        finally
                        {
                            _maxParallelism.Release();
                        }
                    }, stoppingToken);
                }
            }
        }
        finally
        {
            await _dataWriter.FlushAsync(stoppingToken);
            CloseConsumer();
        }
    }

    private async Task ProcessMessageAsync(ConsumeResult<Ignore, string> consumeResult, CancellationToken stoppingToken)
    {
        try
        {
            _logger.LogInformation("Processing message: {Message}", consumeResult.Message.Value);
            var candleData = _parser.Parse(consumeResult.Message.Value);
            await _dataWriter.WriteAsync(CandleData.MeasurementName, candleData.ToFields(), candleData.ToTags(), stoppingToken);
            _consumer.Commit(consumeResult);
            _logger.LogInformation("Processed and queued data for InfluxDB");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing Kafka message");
        }
    }

    private async Task ProcessMessage(ConsumeResult<Ignore, string> consumeResult, CancellationToken stoppingToken)
    {
        _logger.LogInformation("Received message: {Message}", consumeResult.Message.Value);
        var candleData = _parser.Parse(consumeResult.Message.Value);
        await WriteToInfluxDB(candleData, stoppingToken);
        _consumer.Commit(consumeResult);
        _logger.LogInformation("Processed and wrote data to InfluxDB");
    }

    private async Task WriteToInfluxDB(CandleData candleData, CancellationToken stoppingToken)
    {
        await _dataWriter.WriteAsync(
            CandleData.MeasurementName,
            candleData.ToFields(),
            candleData.ToTags(),
            stoppingToken
        );
    }

    private void CloseConsumer()
    {
        _logger.LogInformation("KafkaConsumerService is stopping. Closing consumer...");
        _consumer.Close();
        _logger.LogInformation("KafkaConsumerService has stopped.");
    }

    public override async Task StopAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("KafkaConsumerService is stopping.");
        await base.StopAsync(stoppingToken);
    }

    public override void Dispose()
    {
        _consumer?.Dispose();
        base.Dispose();
    }
}
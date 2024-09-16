using KafkaToInfluxDB.Services;
using Microsoft.Extensions.Logging;
using Polly.CircuitBreaker;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaToInfluxDB.Services;

public interface IDataWriter
{
    Task WriteAsync(string measurement, Dictionary<string, object> fields, Dictionary<string, string> tags, CancellationToken cancellationToken);
    Task FlushAsync(CancellationToken cancellationToken);
}

public class InfluxDBDataWriter : IDataWriter, IDisposable
{
    private readonly IInfluxDBService _influxDbService;
    private readonly ILogger<InfluxDBDataWriter> _logger;
    private readonly ConcurrentQueue<(string, Dictionary<string, object>, Dictionary<string, string>)> _queue;
    private readonly int _batchSize;
    private readonly int _maxQueueSize;
    private readonly Timer _flushTimer;
    private readonly SemaphoreSlim _flushLock = new SemaphoreSlim(1, 1);

    public InfluxDBDataWriter(IInfluxDBService influxDbService, ILogger<InfluxDBDataWriter> logger, AppConfig appConfig)
    {
        _influxDbService = influxDbService;
        _logger = logger;
        _batchSize = appConfig.Batch.Size;
        _maxQueueSize = appConfig.Batch.MaxQueueSize;
        _queue = new ConcurrentQueue<(string, Dictionary<string, object>, Dictionary<string, string>)>();
        _flushTimer = new Timer(FlushTimerCallback, null, TimeSpan.Zero, TimeSpan.FromSeconds(appConfig.Batch.FlushIntervalSeconds));
    }

    public async Task WriteAsync(string measurement, Dictionary<string, object> fields, Dictionary<string, string> tags, CancellationToken cancellationToken)
    {
        if (_queue.Count >= _maxQueueSize)
        {
            _logger.LogWarning("Queue is full. Dropping message.");
            return;
        }

        _queue.Enqueue((measurement, fields, tags));

        if (_queue.Count >= _batchSize)
        {
            await FlushAsync(cancellationToken);
        }
    }

    private void FlushTimerCallback(object? state)
    {
        _ = FlushAsync(CancellationToken.None);
    }

    public async Task FlushAsync(CancellationToken cancellationToken)
    {
        if (await _flushLock.WaitAsync(0))
        {
            try
            {
                var batchToWrite = new List<(string, Dictionary<string, object>, Dictionary<string, string>)>();
                while (batchToWrite.Count < _batchSize && _queue.TryDequeue(out var item))
                {
                    batchToWrite.Add(item);
                }

                if (batchToWrite.Count > 0)
                {
                    await _influxDbService.WriteBatchAsync(batchToWrite, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error flushing batch to InfluxDB");
            }
            finally
            {
                _flushLock.Release();
            }
        }
    }

    public void Dispose()
    {
        _flushTimer?.Dispose();
        _flushLock?.Dispose();
    }
}
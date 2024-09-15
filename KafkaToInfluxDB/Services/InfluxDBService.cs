using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaToInfluxDB.Exceptions;
using System.Linq;

namespace KafkaToInfluxDB.Services;

public class InfluxDBService : IInfluxDBService, IDisposable
{
    private readonly InfluxDBClient _client;
    private readonly WriteApiAsync _writeApiAsync;
    private readonly string _org;
    private readonly string _bucket;
    private readonly string _url;
    private readonly string _token;
    private readonly ILogger<InfluxDBService> _logger;
    private readonly CircuitBreaker _circuitBreaker = new CircuitBreaker();

    public InfluxDBService(AppConfig appConfig, ILogger<InfluxDBService> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        if (appConfig?.InfluxDB == null)
        {
            throw new ArgumentNullException(nameof(appConfig), "InfluxDB configuration is missing");
        }

        _org = appConfig.InfluxDB.Org ?? throw new ArgumentNullException(nameof(appConfig.InfluxDB.Org), "InfluxDB Org is not configured");
        _bucket = appConfig.InfluxDB.Bucket ?? throw new ArgumentNullException(nameof(appConfig.InfluxDB.Bucket), "InfluxDB Bucket is not configured");
        _url = appConfig.InfluxDB.Url ?? throw new ArgumentNullException(nameof(appConfig.InfluxDB.Url), "InfluxDB Url is not configured");
        _token = appConfig.InfluxDB.Token ?? throw new ArgumentNullException(nameof(appConfig.InfluxDB.Token), "InfluxDB Token is not configured");

        if (string.IsNullOrEmpty(_token))
        {
            throw new ArgumentNullException(nameof(_token), "InfluxDB Token is not configured");
        }

        _logger.LogInformation("Initializing InfluxDBService with configuration: {@InfluxDBConfig}", new
        {
            Url = _url,
            Org = _org,
            Bucket = _bucket,
            HasToken = !string.IsNullOrEmpty(_token)
        });

        var options = new InfluxDBClientOptions(_url)
        {
            Token = _token,
            Org = _org,
            Bucket = _bucket
        };

        _client = new InfluxDBClient(options);
        _writeApiAsync = _client.GetWriteApiAsync();
        _logger.LogInformation("InfluxDB client and WriteApiAsync initialized successfully");

        _circuitBreaker = new CircuitBreaker(
            int.Parse(Environment.GetEnvironmentVariable("CIRCUIT_BREAKER_FAILURE_THRESHOLD") ?? "5"),
            int.Parse(Environment.GetEnvironmentVariable("CIRCUIT_BREAKER_BREAK_DURATION_SECONDS") ?? "30")
        );
    }

    public async Task EnsureOrganizationAndBucketExistAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var orgsApi = _client.GetOrganizationsApi();
            var organizations = await orgsApi.FindOrganizationsAsync(cancellationToken: cancellationToken);
            _logger.LogInformation("Found {Count} organizations", organizations.Count);

            var existingOrg = organizations.FirstOrDefault(org => org.Name == _org);

            if (existingOrg == null)
            {
                _logger.LogInformation("Organization '{Org}' does not exist. Creating...", _org);
                existingOrg = await orgsApi.CreateOrganizationAsync(new Organization(name: _org), cancellationToken);
                _logger.LogInformation("Organization '{Org}' created with ID: {Id}", existingOrg.Name, existingOrg.Id);
            }
            else
            {
                _logger.LogInformation("Organization '{Org}' exists with ID: {Id}", existingOrg.Name, existingOrg.Id);
            }

            var bucketsApi = _client.GetBucketsApi();
            var buckets = await bucketsApi.FindBucketsAsync(org: _org, cancellationToken: cancellationToken);
            var bucket = buckets.FirstOrDefault(b => b.Name == _bucket);
            if (bucket == null)
            {
                _logger.LogInformation("Creating bucket '{Bucket}' in organization '{Org}'", _bucket, _org);
                var bucketRequest = new PostBucketRequest(
                    orgID: existingOrg.Id,
                    name: _bucket,
                    retentionRules: new List<BucketRetentionRules>
                    {
                        new BucketRetentionRules(type: BucketRetentionRules.TypeEnum.Expire, everySeconds: 0)
                    }
                );
                bucket = await bucketsApi.CreateBucketAsync(bucketRequest, cancellationToken);
                _logger.LogInformation("Bucket '{Bucket}' created with ID: {Id}", _bucket, bucket.Id);
            }
            else
            {
                _logger.LogInformation("Bucket '{Bucket}' already exists with ID: {Id}", _bucket, bucket.Id);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error ensuring organization and bucket exist");
            throw;
        }
    }

    public async Task WriteRandomDataPointAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var point = PointData.Measurement("random_measurement")
                .Tag("host", "host1")
                .Field("value", Random.Shared.NextDouble())
                .Timestamp(DateTime.UtcNow, WritePrecision.Ns);

            await _writeApiAsync.WritePointAsync(point, _bucket, _org, cancellationToken);
            _logger.LogInformation("Wrote random data point to bucket '{Bucket}'", _bucket);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error writing data point to InfluxDB");
            throw;
        }
    }

    public async Task WriteDataAsync(string measurement, Dictionary<string, object> fields, Dictionary<string, string> tags, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"Writing data to org: {_org}, bucket: {_bucket}");
        
        var point = PointData.Measurement(measurement)
            .Timestamp(DateTime.UtcNow, WritePrecision.Ns);

        foreach (var tag in tags)
        {
            point = point.Tag(tag.Key, tag.Value);
        }

        foreach (var field in fields)
        {
            point = point.Field(field.Key, field.Value);
        }

        int retries = 0;
        int maxRetries = int.Parse(Environment.GetEnvironmentVariable("INFLUXDB_MAX_RETRIES") ?? "3");
        int baseDelay = int.Parse(Environment.GetEnvironmentVariable("INFLUXDB_BASE_DELAY_MS") ?? "1000");

        while (true)
        {
            try
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Write operation canceled due to shutdown.");
                    return;
                }

                if (!_circuitBreaker.AllowExecution())
                {
                    _logger.LogWarning("Circuit breaker is open. Skipping InfluxDB write operation.");
                    return;
                }

                await _writeApiAsync.WritePointAsync(point, _bucket, _org, cancellationToken);
                _logger.LogInformation($"Successfully wrote data point to measurement: {measurement}");
                _circuitBreaker.OnSuccess();
                return;
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Write operation canceled due to shutdown.");
                return;
            }
            catch (Exception ex)
            {
                retries++;
                _circuitBreaker.OnFailure();
                int delay = (int)(baseDelay * Math.Pow(2, retries - 1)); // Exponential backoff

                if (ex is TaskCanceledException || ex is OperationCanceledException)
                {
                    _logger.LogWarning(ex, $"InfluxDB write operation was canceled. Retry {retries} of {maxRetries} in {delay}ms");
                }
                else
                {
                    _logger.LogError(ex, $"Error writing data point to measurement: {measurement}. Retry {retries} of {maxRetries} in {delay}ms");
                }

                if (retries >= maxRetries)
                {
                    _logger.LogError($"Failed to write data point after {maxRetries} attempts. Skipping this data point.");
                    await LogFailedWriteAsync(point, ex);
                    return; // Skip this data point instead of throwing an exception
                }

                try
                {
                    await Task.Delay(delay, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogWarning("Delay between retries was canceled.");
                    return;
                }
            }
        }
    }

    private async Task LogFailedWriteAsync(PointData point, Exception exception)
    {
        try
        {
            var failedWrite = new
            {
                Timestamp = DateTime.UtcNow,
                Measurement = GetPointDataMeasurement(point),
                Fields = GetPointDataFields(point),
                Tags = GetPointDataTags(point),
                Error = exception.Message
            };

            var json = System.Text.Json.JsonSerializer.Serialize(failedWrite);
            await System.IO.File.AppendAllTextAsync("failed_writes.log", json + Environment.NewLine);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to log failed write");
        }
    }

    private string GetPointDataMeasurement(PointData point)
    {
        var measurementProperty = point.GetType().GetProperty("MeasurementName") ?? 
                                  point.GetType().GetProperty("Name") ??
                                  point.GetType().GetProperty("Measurement");
        
        return measurementProperty?.GetValue(point)?.ToString() ?? "Unknown";
    }

    private Dictionary<string, object> GetPointDataFields(PointData point)
    {
        var fields = new Dictionary<string, object>();
        var fieldsProperty = point.GetType().GetProperty("Fields");
        if (fieldsProperty != null)
        {
            var fieldsValue = fieldsProperty.GetValue(point);
            if (fieldsValue is IDictionary<string, object> fieldsDictionary)
            {
                foreach (var kvp in fieldsDictionary)
                {
                    fields[kvp.Key] = kvp.Value;
                }
            }
        }
        return fields;
    }

    private Dictionary<string, string> GetPointDataTags(PointData point)
    {
        var tags = new Dictionary<string, string>();
        var tagsProperty = point.GetType().GetProperty("Tags");
        if (tagsProperty != null)
        {
            var tagsValue = tagsProperty.GetValue(point);
            if (tagsValue is IDictionary<string, string> tagsDictionary)
            {
                foreach (var kvp in tagsDictionary)
                {
                    tags[kvp.Key] = kvp.Value;
                }
            }
        }
        return tags;
    }

    public async Task<bool> CheckHealthAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            // Use the cancellationToken to create a timeout
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(5)); // 5 seconds timeout

            return await _client.PingAsync().WaitAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("InfluxDB health check timed out");
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking InfluxDB health");
            return false;
        }
    }

    public void Dispose()
    {
        _client?.Dispose();
    }

    private class CircuitBreaker
    {
        private int _failureCount;
        private DateTime _lastFailureTime;
        private readonly int _threshold;
        private readonly TimeSpan _breakDuration;
        private readonly object _lock = new object();

        public CircuitBreaker(int threshold = 5, int breakDurationSeconds = 30)
        {
            _threshold = threshold;
            _breakDuration = TimeSpan.FromSeconds(breakDurationSeconds);
        }

        public bool AllowExecution()
        {
            lock (_lock)
            {
                if (_failureCount >= _threshold && DateTime.UtcNow - _lastFailureTime < _breakDuration)
                {
                    return false;
                }
                return true;
            }
        }

        public void OnSuccess()
        {
            lock (_lock)
            {
                _failureCount = 0;
            }
        }

        public void OnFailure()
        {
            lock (_lock)
            {
                _failureCount++;
                _lastFailureTime = DateTime.UtcNow;
            }
        }
    }
}



using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using KafkaToInfluxDB.Models;
using Microsoft.Extensions.Logging;
using KafkaToInfluxDB.Exceptions;
using InfluxDB.Client.Core.Exceptions;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaToInfluxDB.Services;

public interface IInfluxDBService
{
    Task WritePointAsync(CandleData candleData);
    Task EnsureBucketExistsAsync();
    Task<bool> CheckHealthAsync();
}

public class InfluxDBService : IInfluxDBService, IDisposable
{
    private readonly InfluxDBClient _client;
    private readonly string _bucket;
    private readonly string _org;
    private readonly ILogger<InfluxDBService> _logger;
    private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
    private readonly CircuitBreaker _circuitBreaker;

    public InfluxDBService(AppConfig appConfig, ILogger<InfluxDBService> logger)
    {
        _logger = logger;
        _logger.LogInformation("Initializing InfluxDBService with configuration: {@InfluxDBConfig}", new
        {
            Url = appConfig.InfluxDB.Url,
            Bucket = appConfig.InfluxDB.Bucket,
            Org = appConfig.InfluxDB.Org,
            HasToken = !string.IsNullOrEmpty(appConfig.InfluxDB.Token)
        });

        if (string.IsNullOrEmpty(appConfig.InfluxDB.Bucket))
        {
            throw new ArgumentNullException(nameof(appConfig.InfluxDB.Bucket), "InfluxDB Bucket cannot be null or empty");
        }

        if (string.IsNullOrEmpty(appConfig.InfluxDB.Org))
        {
            throw new ArgumentNullException(nameof(appConfig.InfluxDB.Org), "InfluxDB Org cannot be null or empty");
        }

        if (string.IsNullOrEmpty(appConfig.InfluxDB.Token))
        {
            throw new ArgumentNullException(nameof(appConfig.InfluxDB.Token), "InfluxDB Token cannot be null or empty");
        }

        _logger.LogInformation("Attempting to connect to InfluxDB at URL: {Url}", appConfig.InfluxDB.Url);

        var options = InfluxDBClientOptions.Builder
            .CreateNew()
            .Url(appConfig.InfluxDB.Url)
            .AuthenticateToken(appConfig.InfluxDB.Token.ToCharArray())
            .Org(appConfig.InfluxDB.Org)
            .Bucket(appConfig.InfluxDB.Bucket)
            .Build();

        _client = new InfluxDBClient(options);
        _bucket = appConfig.InfluxDB.Bucket;
        _org = appConfig.InfluxDB.Org;
        _circuitBreaker = new CircuitBreaker(3, TimeSpan.FromSeconds(30));
    }

    public async Task WritePointAsync(CandleData candleData)
    {
        var point = PointData.Measurement("candles")
            .Tag("product_id", candleData.ProductId)
            .Field("high", candleData.High)
            .Field("low", candleData.Low)
            .Field("open", candleData.Open)
            .Field("close", candleData.Close)
            .Field("volume", candleData.Volume)
            .Timestamp(DateTimeOffset.FromUnixTimeSeconds(candleData.Start).UtcDateTime, WritePrecision.Ns);

        await _circuitBreaker.ExecuteAsync(async () =>
        {
            await RetryAsync(async () =>
            {
                await _semaphore.WaitAsync();
                try
                {
                    using var writeApi = _client.GetWriteApi();
                    _logger.LogInformation("Attempting to write point: {@Point}", point);
                    await Task.Run(() => writeApi.WritePoint(point, _bucket, _org));
                    _logger.LogInformation("Point written successfully: {@CandleData}", candleData);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error writing point to InfluxDB: {@Point}", point);
                    throw;
                }
                finally
                {
                    _semaphore.Release();
                }
            });
        });
    }

    private async Task RetryAsync(Func<Task> action, int maxRetries = 3)
    {
        Exception? lastException = null;
        for (int i = 0; i < maxRetries; i++)
        {
            try
            {
                await action();
                return;
            }
            catch (Exception ex)
            {
                lastException = ex;
                if (i < maxRetries - 1)
                {
                    _logger.LogWarning(ex, "Error writing point to InfluxDB. Retry attempt {Attempt} of {MaxRetries}", i + 1, maxRetries);
                    await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, i))); // Exponential backoff
                }
            }
        }
        throw new InfluxDBWriteException($"Failed to write point after {maxRetries} attempts", lastException ?? new Exception("Unknown error"));
    }

    public async Task EnsureBucketExistsAsync()
    {
        try
        {
            _logger.LogInformation("Attempting to connect to InfluxDB");
            _logger.LogInformation("Using bucket: {Bucket}", _bucket);

            var bucketsApi = _client.GetBucketsApi();
            _logger.LogInformation("Successfully got BucketsApi");

            await ListOrganizationsAsync();
            await FindOrCreateOrganizationAsync();
            await ListOrganizationsAsync(); // List organizations again after potential creation
            await FindOrCreateBucketAsync(bucketsApi);

            _logger.LogInformation("Successfully connected to InfluxDB and verified bucket existence");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error ensuring bucket exists or connecting to InfluxDB. Exception type: {ExceptionType}", ex.GetType().Name);
            throw;
        }
    }

    private async Task ListOrganizationsAsync()
    {
        var organizationsApi = _client.GetOrganizationsApi();
        var organizations = await organizationsApi.FindOrganizationsAsync();
        _logger.LogInformation("Listed organizations:");
        foreach (var org in organizations)
        {
            _logger.LogInformation("Organization: Name = {Name}, ID = {Id}, Created = {Created}", org.Name, org.Id, org.CreatedAt);
        }
    }

    private async Task FindOrCreateOrganizationAsync()
    {
        var organizationsApi = _client.GetOrganizationsApi();
        var organizations = await organizationsApi.FindOrganizationsAsync(org: _org);

        if (organizations.Count == 0)
        {
            _logger.LogInformation("Organization {Org} not found. Creating new organization.", _org);
            var newOrg = await organizationsApi.CreateOrganizationAsync(_org);
            _logger.LogInformation("Created new organization: {OrgId}", newOrg.Id);
        }
        else
        {
            var organization = organizations.First();
            _logger.LogInformation("Organization {Org} found with ID: {OrgId}", _org, organization.Id);
        }
    }
    private async Task FindOrCreateBucketAsync(BucketsApi bucketsApi)
    {
        try
        {
            var bucket = await bucketsApi.FindBucketByNameAsync(_bucket);
            if (bucket != null)
            {
                _logger.LogInformation("Bucket '{Bucket}' already exists", _bucket);
                return;
            }
        }
        catch (NotFoundException)
        {
            _logger.LogInformation("Bucket '{Bucket}' not found, creating it now...", _bucket);
        }

        var retention = new BucketRetentionRules(BucketRetentionRules.TypeEnum.Expire, 30 * 24 * 60 * 60); // 30 days retention
        var bucketRequest = new PostBucketRequest(
            orgID: (await _client.GetOrganizationsApi().FindOrganizationsAsync()).First().Id,
            name: _bucket,
            retentionRules: new List<BucketRetentionRules> { retention }
        );
        await bucketsApi.CreateBucketAsync(bucketRequest);
        _logger.LogInformation("Bucket '{Bucket}' created successfully", _bucket);
    }

    public async Task<bool> CheckHealthAsync()
    {
        try
        {
            var ping = await _client.PingAsync();
            return ping;
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
        _semaphore?.Dispose();
    }
}

public class CircuitBreaker
{
    private readonly int _failureThreshold;
    private readonly TimeSpan _resetTimeout;
    private int _failureCount;
    private DateTime _lastFailureTime;
    private bool _isOpen;

    public CircuitBreaker(int failureThreshold, TimeSpan resetTimeout)
    {
        _failureThreshold = failureThreshold;
        _resetTimeout = resetTimeout;
    }

    public async Task ExecuteAsync(Func<Task> action)
    {
        if (_isOpen)
        {
            if (DateTime.UtcNow - _lastFailureTime > _resetTimeout)
            {
                _isOpen = false;
                _failureCount = 0;
            }
            else
            {
                throw new CircuitBreakerOpenException("Circuit is open");
            }
        }

        try
        {
            await action();
            _failureCount = 0;
        }
        catch (Exception)
        {
            _failureCount++;
            _lastFailureTime = DateTime.UtcNow;
            if (_failureCount >= _failureThreshold)
            {
                _isOpen = true;
            }
            throw;
        }
    }
}

public class CircuitBreakerOpenException : Exception
{
    public CircuitBreakerOpenException(string message) : base(message) { }
}
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using Microsoft.Extensions.Logging;
using System.Threading;
using KafkaToInfluxDB.Services;

public class InfluxDBService : IInfluxDBService, IAsyncDisposable
{
    private readonly InfluxDBClient _client;
    private readonly WriteApiAsync _writeApiAsync;
    private readonly string _org;
    private readonly string _bucket;
    private readonly ILogger<InfluxDBService> _logger;

    public InfluxDBService(AppConfig appConfig, ILogger<InfluxDBService> logger)
    {
        _logger = logger;
        _org = appConfig.InfluxDB.Org ?? "mytestorg";
        _bucket = appConfig.InfluxDB.Bucket ?? "mytestbucket";

        _logger.LogInformation("Initializing InfluxDBService with configuration: {@InfluxDBConfig}", new
        {
            Url = appConfig.InfluxDB.Url,
            Org = _org,
            Bucket = _bucket,
            HasToken = !string.IsNullOrEmpty(appConfig.InfluxDB.Token)
        });

        if (string.IsNullOrEmpty(appConfig.InfluxDB.Token))
        {
            throw new ArgumentNullException(nameof(appConfig.InfluxDB.Token), "InfluxDB Token is not configured");
        }

        var options = InfluxDBClientOptions.Builder
            .CreateNew()
            .Url(appConfig.InfluxDB.Url)
            .AuthenticateToken(appConfig.InfluxDB.Token.ToCharArray())
            .Org(_org)
            .Build();

        _client = new InfluxDBClient(options);
        _writeApiAsync = _client.GetWriteApiAsync();
        _logger.LogInformation("InfluxDB client and WriteApiAsync initialized successfully");
    }

    public async Task EnsureOrganizationAndBucketExistAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            // Ensure Organization exists
            var orgsApi = _client.GetOrganizationsApi();
            var existingOrg = (await orgsApi.FindOrganizationsAsync(org: _org, cancellationToken: cancellationToken)).FirstOrDefault();
            if (existingOrg == null)
            {
                _logger.LogInformation("Organization '{Org}' does not exist. Creating...", _org);
                existingOrg = await orgsApi.CreateOrganizationAsync(_org, cancellationToken);
                _logger.LogInformation("Organization '{Org}' created with ID: {Id}", existingOrg.Name, existingOrg.Id);
            }
            else
            {
                _logger.LogInformation("Organization '{Org}' exists with ID: {Id}", existingOrg.Name, existingOrg.Id);
            }

            // Ensure Bucket exists
            var bucketsApi = _client.GetBucketsApi();
            var existingBucket = await bucketsApi.FindBucketByNameAsync(_bucket, cancellationToken);
            if (existingBucket == null)
            {
                _logger.LogInformation("Bucket '{Bucket}' does not exist. Creating...", _bucket);
                var retention = new BucketRetentionRules(BucketRetentionRules.TypeEnum.Expire, 0); // Infinite retention
                var bucketRequest = new Bucket(
                    name: _bucket,
                    retentionRules: new List<BucketRetentionRules> { retention },
                    orgID: existingOrg.Id
                );
                existingBucket = await bucketsApi.CreateBucketAsync(bucketRequest, cancellationToken);
                _logger.LogInformation("Bucket '{Bucket}' created with ID: {Id}", existingBucket.Name, existingBucket.Id);
            }
            else
            {
                _logger.LogInformation("Bucket '{Bucket}' exists with ID: {Id}", existingBucket.Name, existingBucket.Id);
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

    public async ValueTask DisposeAsync()
    {
        if (_client is IAsyncDisposable asyncDisposable)
        {
            await asyncDisposable.DisposeAsync();
        }
        else
        {
            _client?.Dispose();
        }
    }
}

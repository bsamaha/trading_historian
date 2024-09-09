using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using Microsoft.Extensions.Configuration;
using KafkaToInfluxDB.Models;
using Microsoft.Extensions.Logging;
using KafkaToInfluxDB.Exceptions;

namespace KafkaToInfluxDB.Services;

public interface IInfluxDBService
{
    Task WritePointAsync(CandleData candleData);
    Task EnsureBucketExistsAsync();
}

public class InfluxDBService : IInfluxDBService, IDisposable
{
    private readonly InfluxDBClient _client;
    private readonly string _bucket;
    private readonly string _org;
    private readonly ILogger<InfluxDBService> _logger;

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

        _client = new InfluxDBClient(appConfig.InfluxDB.Url, appConfig.InfluxDB.Token);
        _bucket = appConfig.InfluxDB.Bucket;
        _org = appConfig.InfluxDB.Org;
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

        try
        {
            using var writeApi = _client.GetWriteApi();
            await Task.Run(() => writeApi.WritePoint(point, _bucket, _org));
            _logger.LogDebug("Point written successfully: {@CandleData}", candleData);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error writing point to InfluxDB: {@CandleData}", candleData);
            throw new InfluxDBWriteException("Error writing point to InfluxDB", ex);
        }
    }

    public async Task EnsureBucketExistsAsync()
    {
        try
        {
            var bucketsApi = _client.GetBucketsApi();
            var org = await _client.GetOrganizationsApi().FindOrganizationsAsync(org: _org);

            if (org == null)
            {
                _logger.LogError("Organization '{Org}' not found", _org);
                throw new InvalidOperationException($"Organization '{_org}' not found.");
            }

            var bucket = await bucketsApi.FindBucketByNameAsync(_bucket);

            if (bucket == null)
            {
                _logger.LogInformation("Bucket '{Bucket}' not found. Creating it now...", _bucket);
                var retention = new BucketRetentionRules(BucketRetentionRules.TypeEnum.Expire, 30 * 24 * 60 * 60); // 30 days retention
                await bucketsApi.CreateBucketAsync(_bucket, retention, org.First().Id);
                _logger.LogInformation("Bucket '{Bucket}' created successfully.", _bucket);
            }
            else
            {
                _logger.LogInformation("Bucket '{Bucket}' already exists.", _bucket);
            }

            _logger.LogInformation("Successfully connected to InfluxDB and verified bucket existence");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error ensuring bucket exists or connecting to InfluxDB");
            throw;
        }
    }

    public void Dispose()
    {
        _client?.Dispose();
    }
}
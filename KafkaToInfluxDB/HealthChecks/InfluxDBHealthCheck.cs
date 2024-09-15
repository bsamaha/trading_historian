using Microsoft.Extensions.Diagnostics.HealthChecks;
using KafkaToInfluxDB.Services;

namespace KafkaToInfluxDB.HealthChecks
{
    public class InfluxDBHealthCheck : IHealthCheck
    {
    private readonly IInfluxDBService _influxDBService;

    public InfluxDBHealthCheck(IInfluxDBService influxDBService)
    {
        _influxDBService = influxDBService;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var isHealthy = await _influxDBService.CheckHealthAsync(cancellationToken);
            return isHealthy 
                ? HealthCheckResult.Healthy("InfluxDB connection is healthy")
                : HealthCheckResult.Unhealthy("InfluxDB connection is unhealthy");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("InfluxDB connection is unhealthy", ex);
        }
        }
    }
}
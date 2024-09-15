namespace KafkaToInfluxDB.Services;

public class DataGeneratorService : IHostedService, IDisposable
{
    private readonly IInfluxDBService _influxDBService;
    private readonly ILogger<DataGeneratorService> _logger;
    private Timer? _timer;

    public DataGeneratorService(IInfluxDBService influxDBService, ILogger<DataGeneratorService> logger)
    {
        _influxDBService = influxDBService;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("DataGeneratorService is starting.");

        await _influxDBService.EnsureOrganizationAndBucketExistAsync();

        _timer = new Timer(GenerateData, null, TimeSpan.Zero, TimeSpan.FromSeconds(1));
    }

    private void GenerateData(object? state)
    {
        _influxDBService.WriteRandomDataPointAsync().GetAwaiter().GetResult();
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("DataGeneratorService is stopping.");
        _timer?.Change(Timeout.Infinite, 0);
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _timer?.Dispose();
    }
}

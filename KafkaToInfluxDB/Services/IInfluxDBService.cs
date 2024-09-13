using System.Threading;
using System.Threading.Tasks;

namespace KafkaToInfluxDB.Services;

public interface IInfluxDBService : IAsyncDisposable
{
    Task EnsureOrganizationAndBucketExistAsync(CancellationToken cancellationToken = default);
    Task WriteRandomDataPointAsync(CancellationToken cancellationToken = default);
    Task<bool> CheckHealthAsync(CancellationToken cancellationToken = default);
}

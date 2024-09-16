using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace KafkaToInfluxDB.Services;

public interface IInfluxDBService : IDisposable
{
    Task EnsureOrganizationAndBucketExistAsync(CancellationToken cancellationToken = default);
    Task WriteRandomDataPointAsync(CancellationToken cancellationToken = default);
    Task<bool> CheckHealthAsync(CancellationToken cancellationToken = default);
    Task WriteDataAsync(string measurement, Dictionary<string, object> fields, Dictionary<string, string> tags, CancellationToken cancellationToken = default);
    Task WriteBatchAsync(List<(string, Dictionary<string, object>, Dictionary<string, string>)> batchToWrite, CancellationToken cancellationToken);
}

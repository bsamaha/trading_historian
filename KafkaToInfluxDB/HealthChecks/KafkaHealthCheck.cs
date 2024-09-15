using Microsoft.Extensions.Diagnostics.HealthChecks;
using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaToInfluxDB.HealthChecks
{
    public class KafkaHealthCheck : IHealthCheck
    {
        private readonly IConsumer<Ignore, string> _consumer;
        private readonly IAdminClient _adminClient;

        public KafkaHealthCheck(IConsumer<Ignore, string> consumer, IAdminClient adminClient)
        {
            _consumer = consumer;
            _adminClient = adminClient;
        }

        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            try
            {
                // Wrap the synchronous ListGroups call in Task.Run to make it asynchronous
                var groups = await Task.Run(() => _adminClient.ListGroups(TimeSpan.FromSeconds(5)), cancellationToken);

                // If we can list groups, the connection is healthy
                return HealthCheckResult.Healthy("Kafka connection is healthy");
            }
            catch (OperationCanceledException)
            {
                return HealthCheckResult.Unhealthy("Kafka health check was cancelled.");
            }
            catch (KafkaException ex)
            {
                return HealthCheckResult.Unhealthy("Kafka connection is unhealthy", ex);
            }
        }
    }
}
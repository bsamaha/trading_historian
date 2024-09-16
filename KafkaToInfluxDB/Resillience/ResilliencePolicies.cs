using System;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using KafkaToInfluxDB.Exceptions;

namespace KafkaToInfluxDB.Resilience
{
    public class ResiliencePolicies
    {
        public AsyncRetryPolicy RetryPolicy { get; }
        public AsyncCircuitBreakerPolicy CircuitBreakerPolicy { get; }

        public ResiliencePolicies(ILogger logger)
        {
            RetryPolicy = Policy
                .Handle<Exception>(ex => !(ex is OperationCanceledException))
                .WaitAndRetryAsync(
                    retryCount: 3,
                    sleepDurationProvider: attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        logger.LogWarning(exception, "Error writing to InfluxDB. Retry attempt {RetryCount} after {SleepDuration}ms", retryCount, timeSpan.TotalMilliseconds);
                    }
                );

            CircuitBreakerPolicy = Policy
                .Handle<Exception>(ex => ex is not OperationCanceledException)
                .CircuitBreakerAsync(
                    exceptionsAllowedBeforeBreaking: 5,
                    durationOfBreak: TimeSpan.FromSeconds(30),
                    onBreak: (exception, timespan) =>
                    {
                        logger.LogWarning(exception, "Circuit breaker opened for {DurationOfBreak}s", timespan.TotalSeconds);
                    },
                    onReset: () =>
                    {
                        logger.LogInformation("Circuit breaker reset");
                    }
                );
        }
    }
}
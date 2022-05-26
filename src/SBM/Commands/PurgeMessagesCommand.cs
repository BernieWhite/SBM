// Copyright (c) Bernard White.
// Licensed under the MIT License.

using Azure.Messaging.ServiceBus;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Management.Automation;
using System.Threading;
using System.Threading.Tasks;

namespace SBM
{
    [Flags]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "Multi-enum for PowerShell parameters.")]
    public enum Queue
    {
        Standard = 1,

        DeadLetter = 2
    }

    [Cmdlet("Purge", "Messages")]
    public sealed class PurgeMessagesCommand : PSCmdlet, IDisposable
    {
        private CancellationTokenSource _CancellationTokenSource;
        private TelemetryConfiguration _TelemetryConfiguration;
        private Task[] _Workers;
        private bool _Disposed;
        private Stopwatch _Timer;

        [Parameter(Mandatory = true)]
        public string ConnectionString { get; set; }

        [Parameter(Mandatory = true)]
        public string TopicName { get; set; }

        [Parameter(Mandatory = true)]
        public string[] SubscriptionName { get; set; }

        [Parameter(Mandatory = true)]
        public Queue Queue { get; set; }

        [Parameter(Mandatory = false)]
        public int Timeout { get; set; }

        [Parameter(Mandatory = false)]
        public int BatchSize { get; set; }

        [Parameter(Mandatory = false)]
        public int Retry { get; set; }

        [Parameter(Mandatory = false)]
        public int? Prefetch { get; set; }

        protected override void BeginProcessing()
        {
            _Timer = Stopwatch.StartNew();
            var timeout = Timeout <= 0 ? 10 : Timeout;
            var batchSize = BatchSize <= 0 ? 1000 : BatchSize;
            var retry = Retry <= 0 ? 5 : Retry;
            var workers = new List<Task>();
            _CancellationTokenSource = new CancellationTokenSource();
            _TelemetryConfiguration = new TelemetryConfiguration();

            var aiConnectionString = Environment.GetEnvironmentVariable("APPLICATIONINSIGHTS_CONNECTION_STRING");
            var aiInstrumentationKey = Environment.GetEnvironmentVariable("APPINSIGHTS_INSTRUMENTATIONKEY");
            if (!string.IsNullOrEmpty(aiConnectionString))
                _TelemetryConfiguration.ConnectionString = aiConnectionString;

            if (!string.IsNullOrEmpty(aiInstrumentationKey) && string.IsNullOrEmpty(aiConnectionString))
                _TelemetryConfiguration.InstrumentationKey = aiInstrumentationKey;

            for (var i = 0; i < SubscriptionName.Length; i++)
            {
                if (Queue.HasFlag(Queue.Standard))
                    workers.Add(Run(ConnectionString, TopicName, SubscriptionName[i], false, timeout, batchSize, retry, Prefetch));
                
                if (Queue.HasFlag(Queue.DeadLetter))
                    workers.Add(Run(ConnectionString, TopicName, SubscriptionName[i], true, timeout, batchSize, retry, Prefetch));
            }
            _Workers = workers.ToArray();
        }

        protected override void StopProcessing()
        {
            _CancellationTokenSource.Cancel();
        }

        protected override void EndProcessing()
        {
            Task.WaitAll(_Workers, _CancellationTokenSource.Token);
            _Timer.Stop();
            Console.WriteLine("Completed");
        }

        private async Task Run(string connectionString, string topicName, string subscriptionName, bool deadLetter, int timeout, int batchSize, int retry, int? prefetch)
        {
            var tc = new TelemetryClient(_TelemetryConfiguration);
            var iteration = 1;
            var client = new ServiceBusClient(connectionString);
            var options = new ServiceBusReceiverOptions
            {
                ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
                SubQueue = deadLetter ? SubQueue.DeadLetter : SubQueue.None
            };
            if (prefetch.HasValue && prefetch.Value >= 0)
                options.PrefetchCount = prefetch.Value;

            var r = client.CreateReceiver(topicName, subscriptionName, options);
            Console.WriteLine($"{r.EntityPath}: Starting receiver (Timeout = {timeout}, BatchSize = {batchSize}, Retry = {retry}, PrefetchCount = {r.PrefetchCount})");
            tc.Context.GlobalProperties.Add("path", r.EntityPath);
            tc.Context.GlobalProperties.Add("namespace", client.FullyQualifiedNamespace);
            try
            {
                while (iteration <= retry)
                {
                    var startTime = _Timer.ElapsedMilliseconds;
                    var maxWaitTime = TimeSpan.FromSeconds(timeout << iteration);
                    IReadOnlyList<ServiceBusReceivedMessage> messages = null;
                    try
                    {
                        Console.WriteLine($"{r.EntityPath}[{iteration}]: Receiving messages with max wait of {maxWaitTime.TotalSeconds} seconds");
                        messages = await r.ReceiveMessagesAsync(batchSize, maxWaitTime, cancellationToken: _CancellationTokenSource.Token);
                        Console.WriteLine($"{r.EntityPath}[{iteration}]: Got {messages.Count} messages");
                    }
                    catch (Exception ex)
                    {
                        messages = null;
                        Console.WriteLine($"{r.EntityPath}[{iteration}]: The following exception occured. {ex.Message}");
                        Console.WriteLine($"{r.EntityPath}[{iteration}]: Stack trace: {ex.StackTrace}");
                        tc.TrackException(ex);
                    }
                    finally
                    {
                        if ((messages == null || messages.Count == 0) && (iteration <= retry))
                        {
                            tc.TrackMetric("deletedMessages", 0);
                            tc.TrackMetric("deletedMessages/sec", 0);

                            iteration++;
                            if (iteration <= retry)
                                Console.WriteLine($"{r.EntityPath}: Increasing wait time for iteration ${iteration}");
                        }
                        else if (messages != null || messages.Count > 0)
                        {
                            tc.TrackMetric("deletedMessages", messages.Count);
                            var duration = (_Timer.ElapsedMilliseconds - startTime) / 1000d;
                            var rate = Math.Round(messages.Count / duration, 2);
                            tc.TrackMetric("deletedMessages/sec", rate);
                            iteration = 1;
                            if (iteration > 1)
                                Console.WriteLine($"{r.EntityPath}: Resetting wait time for iteration ${iteration}");
                        }
                    }
                }
            }
            finally
            {
                Console.WriteLine($"{r.EntityPath}: Closing receiver");
                tc.Flush();
                await r.CloseAsync();
                await client.DisposeAsync();
            }
        }

        private void Dispose(bool disposing)
        {
            if (!_Disposed)
            {
                if (disposing)
                {
                    if (_TelemetryConfiguration != null)
                        _TelemetryConfiguration.Dispose();

                    if (_CancellationTokenSource != null)
                        _CancellationTokenSource.Dispose();
                }
                _Disposed = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}

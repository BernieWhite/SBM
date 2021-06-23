// Copyright (c) Bernard White.
// Licensed under the MIT License.

using Azure.Messaging.ServiceBus;
using System;
using System.Collections.Generic;
using System.Management.Automation;
using System.Threading;
using System.Threading.Tasks;

namespace SBM
{
    [Flags]
    public enum Queue
    {
        Standard = 1,

        DeadLetter = 2
    }

    [Cmdlet("Purge", "Messages")]
    public sealed class PurgeMessagesCommand : PSCmdlet
    {
        private CancellationTokenSource _CancellationTokenSource;
        private Task[] _Workers;

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

        protected override void BeginProcessing()
        {
            var timeout = Timeout <= 0 ? 10 : Timeout;
            var batchSize = BatchSize <= 0 ? 1000 : BatchSize;

            var workers = new List<Task>();
            _CancellationTokenSource = new CancellationTokenSource();

            for (var i = 0; i < SubscriptionName.Length; i++)
            {
                if (Queue.HasFlag(Queue.Standard))
                    workers.Add(Run(ConnectionString, TopicName, SubscriptionName[i], false, timeout, batchSize));
                
                if (Queue.HasFlag(Queue.DeadLetter))
                    workers.Add(Run(ConnectionString, TopicName, SubscriptionName[i], true, timeout, batchSize));
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
            Console.WriteLine("Completed");
        }

        private async Task Run(string connectionString, string topicName, string subscriptionName, bool deadLetter, int timeout, int batchSize)
        {
            var client = new ServiceBusClient(connectionString);
            var r = client.CreateReceiver(topicName, subscriptionName, new ServiceBusReceiverOptions
            {
                ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
                SubQueue = deadLetter ? Azure.Messaging.ServiceBus.SubQueue.DeadLetter : Azure.Messaging.ServiceBus.SubQueue.None
            });

            Console.WriteLine($"{r.EntityPath}: Starting receiver (Timeout = {timeout}, BatchSize = {batchSize})");
            try
            {
                while (true)
                {
                    Console.WriteLine($"{r.EntityPath}: Receiving messages");
                    var m = await r.ReceiveMessagesAsync(batchSize, maxWaitTime: TimeSpan.FromSeconds(timeout), cancellationToken: _CancellationTokenSource.Token);
                    Console.WriteLine($"{r.EntityPath}: Got {m.Count} messages");
                    if (m.Count == 0)
                        return;
                }
            }
            finally
            {
                Console.WriteLine($"{r.EntityPath}: Closing receiver");

                await r.CloseAsync();
                await client.DisposeAsync();
            }
        }
    }
}

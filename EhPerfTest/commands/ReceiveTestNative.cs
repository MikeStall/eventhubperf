using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EHPerfTest
{
    // Test to 
    // a) use the native EventHub SDK, 
    // b) receive all the events, 
    // c) verify their payloads conform to what we did in Add(). (no duplicates, exactly once, etc) 
    // 
    public class ReceiveTestNative : IEventProcessorFactory, IEventProcessor
    {
        EventProcessorHost _eventProcessorHost;
        EventProcessorOptions _options;

        int _expected;
        int[] _received; // Track how many we received by index 
        int _totalReceived;

        Stopwatch _stopwatch = new Stopwatch();

        public ReceiveTestNative(
            string path,
            string connectionString,
            string storageConnectionString,
            int expected)
        {
            _expected = expected;
            this._received = new int[expected];

            string eventProcessorHostName = Guid.NewGuid().ToString();

            _options = new EventProcessorOptions
            {
                MaxBatchSize = 1000
            };

            _eventProcessorHost = new EventProcessorHost(
                eventProcessorHostName,
                path,
                EventHubConsumerGroup.DefaultGroupName,
                connectionString,
                storageConnectionString);
        }

        // Drain all events. 
        public async Task WorkAsync()
        {
            Console.WriteLine("Begin test ...  expecting to pull down {0} events", _expected);
            _stopwatch.Start();
            await this._eventProcessorHost.RegisterEventProcessorFactoryAsync(this, _options);

            Console.WriteLine("Waiting while test runs.... Press any key to skip ");
            Console.ReadLine();

            _eventProcessorHost.UnregisterEventProcessorAsync().Wait();
        }

        public IEventProcessor CreateEventProcessor(PartitionContext context)
        {
            return this;
        }

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.WriteLine("Processor Shutting Down. Partition '{0}', Reason: '{1}'.", context.Lease.PartitionId, reason);
            if (reason == CloseReason.Shutdown)
            {
                await context.CheckpointAsync();
            }
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            Console.WriteLine("SimpleEventProcessor initialized.  Partition: '{0}', Offset: '{1}'", context.Lease.PartitionId, context.Lease.Offset);
            return Task.FromResult<object>(null);
        }

        public void Dump()
        {
            using (var tw = new StreamWriter(@"c:\temp\x1.csv"))
            {
                tw.WriteLine("idx, count");
                for (int i = 0; i < _received.Length; i++)
                {
                    tw.WriteLine("{0}, {1}", i, _received[i]);
                }
            }
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
#if false
            foreach (EventData eventData in messages)
            {
                string data = Encoding.UTF8.GetString(eventData.GetBytes());

                Console.WriteLine(string.Format("Message received.  Partition: '{0}', Data: '{1}'",
                    context.Lease.PartitionId, data));
            }
#else
            int count = 0;
            foreach (var e in messages)
            {
                count++;
                var str = Encoding.UTF8.GetString(e.GetBytes());
                var item = JsonConvert.DeserializeObject<Item>(str);

                var c = Interlocked.Increment(ref _received[item.Num]);
                if (c == 2)
                {
                    // Double counted!!!
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("*** Error. duplicate event: {0}", item.Num);
                    Console.ResetColor();
                }
            }

            lock (this)
            {
                _totalReceived += count;

                if (_totalReceived == _expected)
                {
                    // !!! Done
                    var time = _stopwatch.ElapsedMilliseconds;

                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine(">>>>  Done! Read {0} events in {1}s", _totalReceived, time / 1000);
                    Console.ResetColor();
                }
            }


            Console.WriteLine("[{1}], Got: {0} events. total={2}", count, DateTime.UtcNow, _totalReceived);
#endif

            //Call checkpoint every 5 minutes, so that worker can resume processing from 5 minutes back if it restarts.
            //if (this.checkpointStopWatch.Elapsed > TimeSpan.FromMinutes(5))
            {
                await context.CheckpointAsync();
            }
        }
    }
}
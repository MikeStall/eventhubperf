using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EHPerfTest
{
    // Drain all the events in a hub for diagnostic purposes. 
    public class DrainEvent :  IEventProcessorFactory
    {
        EventProcessorHost _eventProcessorHost;
        EventProcessorOptions _options;

        public DrainEvent(
            string path, 
            string connectionString, 
            string storageConnectionString)
        {
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

        int _totalEvents;
        Stopwatch _stopwatch = new Stopwatch();

        // Drain all events. 
        public async Task WorkAsync()
        {
            Console.WriteLine("Draining events... will stop listening when there are no events for 60 seconds");
            _stopwatch.Start();
            await this._eventProcessorHost.RegisterEventProcessorFactoryAsync(this, _options);


            while (true)
            {
                await Task.Delay(3000);
                if (_stopwatch.ElapsedMilliseconds > 40 * 1000)
                {
                    Console.WriteLine("no events left ....");
                    break;
                }
                Console.Write(".");
            }
            
            _eventProcessorHost.UnregisterEventProcessorAsync().Wait();
        }

        public IEventProcessor CreateEventProcessor(PartitionContext context)
        {
            return new SimpleEventProcessor(this);
        }

        class SimpleEventProcessor : IEventProcessor
        {
            Stopwatch checkpointStopWatch;
            private readonly DrainEvent _parent;

            public SimpleEventProcessor(DrainEvent outer)
            {
                _parent = outer;
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
                this.checkpointStopWatch = new Stopwatch();
                this.checkpointStopWatch.Start();
                return Task.FromResult<object>(null);
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
                int count = messages.Count();
                lock(_parent)
                {
                    _parent._totalEvents += count; ;
                    _parent._stopwatch.Restart();
                }

                Console.WriteLine("[{1}], Got: {0} events", count, DateTime.UtcNow);
#endif

                //Call checkpoint every 5 minutes, so that worker can resume processing from 5 minutes back if it restarts.
                //if (this.checkpointStopWatch.Elapsed > TimeSpan.FromMinutes(5))
                {
                    await context.CheckpointAsync();
                    this.checkpointStopWatch.Restart();
                }
            }
        }
    }
}
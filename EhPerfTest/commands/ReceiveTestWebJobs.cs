using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.ServiceBus;
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
    public class ReceiveTestWebJobs 
    {
        JobHost _host;

        int _expected;
        int[] _received; // Track how many we received by index 
        int _totalReceived;

        Stopwatch _stopwatch = new Stopwatch();

        public ReceiveTestWebJobs(
            string path,
            string connectionString,
            string storageConnectionString,
            int expected)
        {
            this._received = new int[expected];
            this._expected = expected;

            JobHostConfiguration config = new JobHostConfiguration();

            // Disable logging 
            config.DashboardConnectionString = null;

            var eventHubConfig = new EventHubConfiguration();
            eventHubConfig.AddReceiver(path, connectionString, storageConnectionString);

            config.UseEventHub(eventHubConfig);
            config.TypeLocator = new FakeTypeLocator(typeof(Program));
            config.JobActivator = new JobActivator().Add(new Program(this));

            var nm = new DictNameResolver();
            nm.Add("eh", path);
            config.NameResolver = nm;

            //config.UseTimers();
            _host = new JobHost(config);
        }

        public async Task WorkAsync()
        {
            _stopwatch.Start();
            _host.RunAndBlock();
        }


        private void HandleItems(Item[] items)
        {
            int count = 0;
            foreach (var item in items)
            {
                count++;
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
                Console.WriteLine("At {0} total events", _totalReceived);

                if (_totalReceived == _expected)
                {
                    // !!! Done
                    var time = _stopwatch.ElapsedMilliseconds;

                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine(">>>>  Done! Read {0} events in {1}s", _totalReceived, time / 1000);
                    Console.ResetColor();
                }
            }
        }

        public class Program
        {
            ReceiveTestWebJobs _parent;

            public Program(ReceiveTestWebJobs parent)
            {
                _parent = parent;
            }

            public void Work(
                [EventHubTrigger("%eh%")] Item[] items)
            {
                _parent.HandleItems(items);             
            }
        }



  
    }
}
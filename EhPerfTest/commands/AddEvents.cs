using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EHPerfTest
{
    // Add a bunch of events 
    public class AddEvents
    {
        EventHubClient _client;

        public AddEvents(
         string path,
         string connectionString
         )
        {
            _client = EventHubClient.CreateFromConnectionString(
                connectionString, path);
        }

        public async Task WorkAsync(int numberToSend)
        {
            Console.WriteLine("Adding {0} events to hub {1}...",
                numberToSend, _client.Path);

            List<EventData> events = new List<EventData>();
            for (int i = 0; i < numberToSend; i++)
            {
                var obj = new Item {
                    Computer = Environment.MachineName,
                    HostId = Program.HostId,
                    Num = i
                };
                var message = JsonConvert.SerializeObject(obj);
                var ed = new EventData(Encoding.UTF8.GetBytes(message));

                events.Add(ed);

                if (events.Count > 100)
                {
                    await _client.SendBatchAsync(events);
                    events.Clear();
                }
                Console.Write(".");
            }

            if (events.Count > 0)
            {
                await _client.SendBatchAsync(events);
            }
        }
    }

    public class Item
    {
        public string Computer
        {
            get; set;
        }
        public string HostId { get; set; }
        public int Num { get; set; }
    }
}
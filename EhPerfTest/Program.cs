using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EHPerfTest
{
    class Program
    {
        public static string HostId = Guid.NewGuid().ToString();
        enum Mode
        {
            drain,
            add,
            testnative,
        }

        // Performance test for EventHub reading 
        static void Main(string[] args)
        {
            string mode = args[0];
            Mode m2 = (Mode) Enum.Parse(typeof(Mode), mode, ignoreCase: true);

            string path = args[1];
            string connectionString = args[2];

            switch (m2)
            {
                case Mode.drain:
                    {     
                        string storageConnectionString = args[3];

                        var drain = new DrainEvent(path, connectionString, storageConnectionString);
                        drain.WorkAsync().Wait();
                    }
                    break;
                case Mode.add:
                    {
                        var add = new AddEvents(path, connectionString);

                        int numEventsToAdd = 10 * 1000;
                                                
                        add.WorkAsync(numEventsToAdd).Wait();
                    }
                    break;

                case Mode.testnative:
                    {
                        string storageConnectionString = args[3];
                        var drain = new DrainEvent(path, connectionString, storageConnectionString);
                        drain.WorkAsync().Wait();

                        int numEventsToAdd = 30 * 1000;

                        var add = new AddEvents(path, connectionString);
                        add.WorkAsync(numEventsToAdd).Wait();

                        var test = new ReceiveTestNative(path, connectionString, storageConnectionString, numEventsToAdd);
                        test.WorkAsync().Wait();
                    }
                    break;
            }
            
        }
    }
}

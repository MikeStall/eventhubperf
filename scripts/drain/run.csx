// Time how quikcly we can drain events from an EH. 
// This has some additional overhead from doing a File write on each event. 

using System;
using System.Threading;


static int[] _received  = new int[100*1000];
static int _total;
static object _lock = new object();
static DateTime _min = DateTime.MaxValue;

public static void Run(Item[] items, TraceWriter log)
{
    // log.Info($"CGDefault: Message: {myEventHubMessage.Length}");
    
    foreach(var item in items)
    {
      Interlocked.Increment(ref _received[item.Num]);
    }
    
    lock(_lock)
    {
      var now = DateTime.UtcNow;
      if (now < _min)
      {
      _min = now;
      }      
    
      _total+= items.Length;        
      string contents = string.Format("{0}, {0},{1}, {2}", _total, now, (now - _min).TotalSeconds);
      
      
      File.WriteAllText(@"C:\temp\r1.txt", contents);
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
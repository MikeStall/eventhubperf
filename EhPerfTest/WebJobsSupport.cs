using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
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
    public class JobActivator : IJobActivator
    {
        private Dictionary<Type, object> _objs = new Dictionary<Type, object>();

        public JobActivator Add<T>(T instance)
        {
            _objs[typeof(T)] = instance;
            return this;
        }

        public T CreateInstance<T>()
        {
            return (T) _objs[typeof(T)];
        }
    }

    public class FakeTypeLocator : ITypeLocator
    {
        private readonly Type[] _types;

        public FakeTypeLocator(params Type[] types)
        {
            _types = types;
        }

        public IReadOnlyList<Type> GetTypes()
        {
            return _types;
        }
    }

    class DictNameResolver : INameResolver
    {
        public Dictionary<string, string> _dict = new Dictionary<string, string>();

        public void Add(string name, string value)
        {
            _dict.Add(name, value);
        }

        public string Resolve(string name)
        {
            string val;
            _dict.TryGetValue(name, out val);
            return val;
        }
    }

}
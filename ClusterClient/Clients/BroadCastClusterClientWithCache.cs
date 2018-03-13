using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    class BroadCastClusterClientWithCache : ClusterClientBase
    {
        private readonly ConcurrentDictionary<string, string> Cache = new ConcurrentDictionary<string, string>();
        public BroadCastClusterClientWithCache(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            if (Cache.ContainsKey(query)) return Cache[query];
            var webRequests = ReplicaAddresses.Select(uri => CreateRequest(uri + "?query=" + query));
            var resultTasks = new List<Task<string>>();
            foreach (var request in webRequests)
            {
                Log.InfoFormat("Processing {0}", request.RequestUri);
                resultTasks.Add(ProcessRequestAsync(request));
            }
            var resultTask = Task.WhenAny(resultTasks);
            await Task.WhenAny(resultTask, Task.Delay(timeout));
            if (!resultTask.IsCompleted && !resultTask.IsFaulted)
                throw new TimeoutException();

            var requestResult = resultTask.Result.Result;
            Cache[query] = requestResult;
            return requestResult;
        }

        protected override ILog Log => LogManager.GetLogger(typeof(BroadCastClusterClientWithCache));
    }
}

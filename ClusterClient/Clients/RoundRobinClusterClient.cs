using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    class RoundRobinClusterClient : ClusterClientBase
    {
        public RoundRobinClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }
        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var time = TimeSpan.FromSeconds(timeout.TotalSeconds / ReplicaAddresses.Length);

            var shuffledAddresses = ReplicaAddresses.Shuffle();
            var webRequests = shuffledAddresses.Select(uri => CreateRequest(uri + "?query=" + query));

            foreach (var request in webRequests)
            {
                Log.InfoFormat("Processing {0}", request.RequestUri);
                var task = ProcessRequestAsync(request);
                await Task.WhenAny(task, Task.Delay(time));
                if (task.IsCompleted && !task.IsFaulted) return task.Result;
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
    }
}

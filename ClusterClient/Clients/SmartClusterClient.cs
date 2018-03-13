using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    class SmartClusterClient : ClusterClientBase
    {
        public SmartClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            timeout = TimeSpan.FromSeconds(timeout.TotalSeconds / ReplicaAddresses.Length);

            var shuffledAddresses = ReplicaAddresses.Shuffle();
            var webRequests = shuffledAddresses.Select(uri => CreateRequest(uri + "?query=" + query));
            var tasks = new List<Task<string>>();

            foreach (var request in webRequests)
            {
                Log.InfoFormat("Processing {0}", request.RequestUri);
                tasks.Add(ProcessRequestAsync(request));
                var task = Task.WhenAny(tasks);
                await Task.WhenAny(task, Task.Delay(timeout));
                if (task.IsCompleted && !task.IsFaulted) return task.Result.Result;
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
    }
}

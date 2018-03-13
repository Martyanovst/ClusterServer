using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    class BroadcastClusterClient : ClusterClientBase
    {
        public BroadcastClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
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

            return resultTask.Result.Result;
        }

        protected override ILog Log => LogManager.GetLogger(typeof(BroadcastClusterClient));
    }
}

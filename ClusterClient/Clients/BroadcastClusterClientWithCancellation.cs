using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Fclp.Internals.Extensions;
using log4net;

namespace ClusterClient.Clients
{
    class BroadCastClusterClientWithCancellation : ClusterClientBase
    {
        public BroadCastClusterClientWithCancellation(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var webRequests = ReplicaAddresses.Select(uri => CreateRequest(uri + "?query=" + query)).ToList();
            var resultTasks = new List<Task<string>>();
            foreach (var request in webRequests)
            {
                Log.InfoFormat("Processing {0}", request.RequestUri);
                resultTasks.Add(ProcessRequestAsync(request));
            }
            var result = Task.WhenAny(resultTasks);
            await Task.WhenAny(result, Task.Delay(timeout));

            if (!result.IsCompleted && !result.IsFaulted)
                throw new TimeoutException();

            for (var i = 0; i < resultTasks.Count; i++)
            {
                if (resultTasks[i].IsCompleted) continue;
                var cancellation = CreateRequest(ReplicaAddresses[i] + "?query=" + query);
                cancellation.Method = "DELETE";
                ProcessRequestAsync(cancellation);
            }

            return result.Result.Result;
        }

        protected override ILog Log => LogManager.GetLogger(typeof(BroadCastClusterClientWithCancellation));
    }
}

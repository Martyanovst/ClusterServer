using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    class GrayListClusterClient : ClusterClientBase
    {
        private readonly ConcurrentDictionary<string, DateTime> mutedServers = new ConcurrentDictionary<string, DateTime>();
        public TimeSpan TimeInBan = TimeSpan.FromSeconds(0.5);
        public GrayListClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
            foreach (var address in replicaAddresses)
                mutedServers[address] = new DateTime();
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            timeout = TimeSpan.FromSeconds(timeout.TotalSeconds / ReplicaAddresses.Length);

            var shuffledAddresses = ReplicaAddresses.Shuffle();
            var filtredAddresses = shuffledAddresses.Where(server => DateTime.Now - mutedServers[server] > TimeInBan).ToList();

            if (filtredAddresses.Count == 0)
                filtredAddresses = shuffledAddresses.OrderBy(server => DateTime.Now - mutedServers[server]).ToList();

            var webRequests = filtredAddresses.Select(uri => CreateRequest(uri + "?query=" + query));

            foreach (var request in webRequests)
            {
                Log.InfoFormat("Processing {0}", request.RequestUri);
                var task = ProcessRequestAsync(request);
                await Task.WhenAny(task, Task.Delay(timeout));
                if (task.IsCompleted && !task.IsFaulted) return task.Result;
                mutedServers[request.Host] = DateTime.Now;
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(GrayListClusterClient));
    }
}

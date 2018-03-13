using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    class RRClusterClientWithPriority : ClusterClientBase
    {
        private readonly ConcurrentDictionary<string,SlidingWindowStatistic> requestStatistics = 
                                           new ConcurrentDictionary<string, SlidingWindowStatistic>();

        private readonly Stopwatch timer = new Stopwatch();
        public RRClusterClientWithPriority(string[] replicaAddresses) : base(replicaAddresses)
        {
            foreach (var address in replicaAddresses)
                requestStatistics[address] = new SlidingWindowStatistic(10);
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            timeout = TimeSpan.FromSeconds(timeout.TotalSeconds / ReplicaAddresses.Length);

            var orderedAddresses = ReplicaAddresses.OrderBy(addr => requestStatistics[addr].GetValue()).ToArray();

            foreach (var uri in orderedAddresses)
            {
                var request = CreateRequest(uri + "?query=" + query);
                Log.InfoFormat("Processing {0}", request.RequestUri);

                timer.Start();
                var task = ProcessRequestAsync(request);
                await Task.WhenAny(task, Task.Delay(timeout));
                timer.Stop();

                requestStatistics[uri].Update(timer.ElapsedMilliseconds);

                timer.Reset();

                if (task.IsCompleted && !task.IsFaulted) return task.Result;
            }
            throw new TimeoutException();
        }


        protected override ILog Log => LogManager.GetLogger(typeof(RRClusterClientWithPriority));
    }
}

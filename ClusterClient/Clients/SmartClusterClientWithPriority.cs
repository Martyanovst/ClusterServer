using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Fclp.Internals.Extensions;
using log4net;

namespace ClusterClient.Clients
{
    class SmartClusterClientWithPriority : ClusterClientBase
    {
        private readonly Dictionary<string, SlidingWindowStatistic> requestStatistics = new Dictionary<string, SlidingWindowStatistic>();
        private readonly Dictionary<string, Stopwatch> stopwatches = new Dictionary<string, Stopwatch>();
        public SmartClusterClientWithPriority(string[] replicaAddresses) : base(replicaAddresses)
        {
            foreach (var address in replicaAddresses)
            {
                requestStatistics[address] = new SlidingWindowStatistic(10);
                stopwatches[address] = new Stopwatch();
            }
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            timeout = TimeSpan.FromSeconds(timeout.TotalSeconds / ReplicaAddresses.Length);

            var orderedAddresses = ReplicaAddresses.OrderBy(addr => requestStatistics[addr].GetValue()).ToArray();

            orderedAddresses.ForEach(uri => stopwatches[uri] = new Stopwatch());

            var tasksPool = new Dictionary<string, Task<string>>();

            foreach (var uri in orderedAddresses)
            {
                var request = CreateRequest(uri + "?query=" + query);
                Log.InfoFormat("Processing {0}", request.RequestUri);

                tasksPool[uri] = ProcessRequestAsync(request);
                var task = Task.WhenAny(tasksPool.Values);
                await Task.WhenAny(task, Task.Delay(timeout));

                if (task.IsCompleted && !task.IsFaulted)
                {
                    stopwatches.Values.ForEach(timer => timer.Stop());

                    var completed = tasksPool.First(pair => pair.Value.IsCompleted).Key;

                    requestStatistics[completed].Update(stopwatches[completed].ElapsedMilliseconds);

                    stopwatches.Values.ForEach(timer => timer.Reset());
                    return task.Result.Result;
                }

                stopwatches[uri].Stop();
                requestStatistics[uri].Update(stopwatches[uri].ElapsedMilliseconds);
                stopwatches[uri].Start();
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
    }
}

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using ClusterClient.Clients;
using Fclp;
using log4net;
using log4net.Config;

namespace ClusterClient
{
    class Program
    {
        static void Main(string[] args)
        {
            XmlConfigurator.Configure();
            if (!TryGetReplicaAddresses(args, out var replicaAddresses, out var delay))
                return;
            try
            {
                var clients = new ClusterClientBase[]
                              {
                                  new RandomClusterClient(replicaAddresses),
                                  new BroadcastClusterClient(replicaAddresses),
                                  new RoundRobinClusterClient(replicaAddresses),
                                  new SmartClusterClient(replicaAddresses),
                                  new GrayListClusterClient(replicaAddresses),
                                  new RRClusterClientWithPriority(replicaAddresses),
                                  new SmartClusterClientWithPriority(replicaAddresses),
                                  new BroadCastClusterClientWithCancellation(replicaAddresses),
                                  new BroadCastClusterClientWithCache(replicaAddresses),
                              };
                var queries = new[] { "От", "топота", "копыт", "пыль", "по", "полю", "летит", "На", "дворе", "трава", "на", "траве", "дрова" };

                foreach (var client in clients)
                {
                    Log.Info(client.GetType());
                    Console.WriteLine("Testing {0} started", client.GetType());
                    Task.WaitAll(queries.Select(
                        async query =>
                        {
                            var timer = Stopwatch.StartNew();
                            try
                            {
                                await client.ProcessRequestAsync(query, TimeSpan.FromSeconds(delay));

                                Console.WriteLine("Processed query \"{0}\" in {1} ms", query, timer.ElapsedMilliseconds);
                            }
                            catch (TimeoutException)
                            {
                                Console.WriteLine("Query \"{0}\" timeout ({1} ms)", query, timer.ElapsedMilliseconds);
                            }
                            timer.Reset();
                        }).ToArray());
                    Task.WaitAll(queries.Select(
                        async query =>
                        {
                            var timer = Stopwatch.StartNew();
                            try
                            {
                                await client.ProcessRequestAsync(query, TimeSpan.FromSeconds(delay));

                                Console.WriteLine("Processed query \"{0}\" in {1} ms", query, timer.ElapsedMilliseconds);
                            }
                            catch (TimeoutException)
                            {
                                Console.WriteLine("Query \"{0}\" timeout ({1} ms)", query, timer.ElapsedMilliseconds);
                            }
                            timer.Reset();
                        }).ToArray());
                    Console.WriteLine("Testing {0} finished", client.GetType());
                }
            }
            catch (Exception e)
            {
                Log.Fatal(e);
            }
        }

        private static bool TryGetReplicaAddresses(string[] args, out string[] replicaAddresses, out double delay)
        {
            var argumentsParser = new FluentCommandLineParser();
            string[] result = { };
            var time = 3;

            argumentsParser.Setup<string>('f', "file")
                .WithDescription("Path to the file with replica addresses")
                .Callback(fileName => result = File.ReadAllLines(fileName))
                .Required();

            argumentsParser.Setup<int>('d', "Delay").Callback(number => time = number);

            argumentsParser.SetupHelp("?", "h", "help")
                .Callback(text => Console.WriteLine(text));

            var parsingResult = argumentsParser.Parse(args);
            delay = time;
            if (parsingResult.HasErrors)
            {
                argumentsParser.HelpOption.ShowHelp(argumentsParser.Options);
                replicaAddresses = null;
                return false;
            }

            replicaAddresses = result;
            return !parsingResult.HasErrors;
        }

        private static readonly ILog Log = LogManager.GetLogger(typeof(Program));
    }
}

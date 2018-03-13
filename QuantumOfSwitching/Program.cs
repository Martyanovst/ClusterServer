using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Text;
using System.Threading.Tasks;

namespace FindTimeQuant
{
	class Program
	{
		private static long counter = 0;
		static void DO()
		{
			while(true)
			{
			}
		}

		static void Main(string[] args)
		{
			var processorNum = args.Length > 0 ? int.Parse(args[0]) - 1 : 0;
			Process.GetCurrentProcess().ProcessorAffinity = (IntPtr)(1 << processorNum);

			new Thread(DO) { IsBackground = true }.Start();

			var sw = Stopwatch.StartNew();
			var lastSeenTime = sw.ElapsedMilliseconds;

			var samples = new List<long>();

			while(true)
			{
				var curTime = sw.ElapsedMilliseconds;
				if(curTime - lastSeenTime > resolutionMs)
					samples.Add(curTime - lastSeenTime);

				lastSeenTime = curTime;
				if(curTime > testDurationMs)
					break;
			}

			foreach(var sample in samples)
				Console.WriteLine(sample);
			Console.WriteLine("Done. ");
		}

		const int resolutionMs = 1;
		const int testDurationMs = 5000;
	}
}
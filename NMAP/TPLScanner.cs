using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using log4net;

namespace NMAP
{
	public class TPLScanner : SequentialScanner
	{
		protected override ILog log => LogManager.GetLogger(typeof(TPLScanner));

		public override Task Scan(IPAddress[] ipAddrs, int[] ports)
		{
			return Task.WhenAll(ipAddrs.Select(ipAddr => ProcessIpAddr(ipAddr, ports)));
		}

		private Task ProcessIpAddr(IPAddress ipAddr, int[] ports)
		{
			return Task.Run(() => PingAddr(ipAddr))
					.ContinueWith(pingTask =>
					{
						if(pingTask.Result != IPStatus.Success)
							return;

						foreach(var port in ports)
							Task.Factory.StartNew(
								() => CheckPort(ipAddr, port),
								TaskCreationOptions.AttachedToParent);
					});
		}
	}
}
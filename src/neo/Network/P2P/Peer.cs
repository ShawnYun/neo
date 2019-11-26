using Akka.Actor;
using Akka.IO;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Neo.IO;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Threading.Tasks;

namespace Neo.Network.P2P
{
    public abstract class Peer : UntypedActor
    {
        /// <summary>
        /// 获取到的节点信息列表
        /// </summary>
        public class Peers { public IEnumerable<IPEndPoint> EndPoints; }
        public class Connect { public IPEndPoint EndPoint; public bool IsTrusted = false; }
        private class Timer { }
        private class WsConnected { public WebSocket Socket; public IPEndPoint Remote; public IPEndPoint Local; }

        /// <summary>
        /// 最少需求连接数
        /// </summary>
        public const int DefaultMinDesiredConnections = 10;
        public const int DefaultMaxConnections = DefaultMinDesiredConnections * 4;

        /// <summary>
        /// TCP的actor
        /// </summary>
        private static readonly IActorRef tcp_manager = Context.System.Tcp();
        private IActorRef tcp_listener;
        private IWebHost ws_host;
        private ICancelable timer;

        /// <summary>
        /// 维护所有已连接的节点actor
        /// </summary>
        protected ActorSelection Connections => Context.ActorSelection("connection_*");

        private static readonly HashSet<IPAddress> localAddresses = new HashSet<IPAddress>();
        /// <summary>
        /// 保存与每个地址建立的连接数的字典
        /// </summary>
        private readonly Dictionary<IPAddress, int> ConnectedAddresses = new Dictionary<IPAddress, int>();
        /// <summary>
        /// Actor引用与已连接IPEndPoint对应关系的字典
        /// </summary>
        protected readonly ConcurrentDictionary<IActorRef, IPEndPoint> ConnectedPeers = new ConcurrentDictionary<IActorRef, IPEndPoint>();

        /// <summary>
        /// 已保存但未连接的IPEndPoint集合
        /// </summary>
        protected ImmutableHashSet<IPEndPoint> UnconnectedPeers = ImmutableHashSet<IPEndPoint>.Empty;
        /// <summary>
        /// 正在连接的IPEndPoint集合，未收到确认
        /// </summary>
        protected ImmutableHashSet<IPEndPoint> ConnectingPeers = ImmutableHashSet<IPEndPoint>.Empty;
        /// <summary>
        /// 可信IP地址集合
        /// </summary>
        protected HashSet<IPAddress> TrustedIpAddresses { get; } = new HashSet<IPAddress>();

        public int ListenerTcpPort { get; private set; }
        public int ListenerWsPort { get; private set; }
        /// <summary>
        /// 每个地址的最大连接数
        /// </summary>
        public int MaxConnectionsPerAddress { get; private set; } = 3;
        /// <summary>
        /// 最少需要连接数
        /// </summary>
        public int MinDesiredConnections { get; private set; } = DefaultMinDesiredConnections;
        /// <summary>
        /// 最大连接数
        /// </summary>
        public int MaxConnections { get; private set; } = DefaultMaxConnections;
        /// <summary>
        /// UnconnectedPeers的最大个数
        /// </summary>
        protected int UnconnectedMax { get; } = 1000;
        protected virtual int ConnectingMax
        {
            get
            {
                var allowedConnecting = MinDesiredConnections * 4;
                allowedConnecting = MaxConnections != -1 && allowedConnecting > MaxConnections
                    ? MaxConnections : allowedConnecting;
                return allowedConnecting - ConnectedPeers.Count;
            }
        }

        static Peer()
        {
            localAddresses.UnionWith(NetworkInterface.GetAllNetworkInterfaces().SelectMany(p => p.GetIPProperties().UnicastAddresses).Select(p => p.Address.Unmap()));
        }

        /// <summary>
        /// 收到RemoteNode接收到的节点信息，添加节点至UnconnectedPeers
        /// </summary>
        /// <param name="peers"></param>
        protected void AddPeers(IEnumerable<IPEndPoint> peers)
        {
            if (UnconnectedPeers.Count < UnconnectedMax)
            {
                //过滤条件
                peers = peers.Where(p => p.Port != ListenerTcpPort || !localAddresses.Contains(p.Address));
                ImmutableInterlocked.Update(ref UnconnectedPeers, p => p.Union(peers));
            }
        }

        /// <summary>
        /// 建立到指定节点的TCP连接
        /// </summary>
        /// <param name="endPoint"></param>
        /// <param name="isTrusted"></param>
        protected void ConnectToPeer(IPEndPoint endPoint, bool isTrusted = false)
        {
            // Unmap用于将IPV4映射IPV6地址转为IPV4地址
            endPoint = endPoint.Unmap();
            if (endPoint.Port == ListenerTcpPort && localAddresses.Contains(endPoint.Address)) return;

            if (isTrusted) TrustedIpAddresses.Add(endPoint.Address);
            //如果与该IP地址的连接数大于等于3个时，返回
            if (ConnectedAddresses.TryGetValue(endPoint.Address, out int count) && count >= MaxConnectionsPerAddress)
                return;
            if (ConnectedPeers.Values.Contains(endPoint)) return;
            ImmutableInterlocked.Update(ref ConnectingPeers, p =>
            {
                if ((p.Count >= ConnectingMax && !isTrusted) || p.Contains(endPoint)) return p;
                tcp_manager.Tell(new Tcp.Connect(endPoint));
                return p.Add(endPoint);
            });
        }

        /// <summary>
        /// 判断是否为局域网地址
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        private static bool IsIntranetAddress(IPAddress address)
        {
            byte[] data = address.MapToIPv4().GetAddressBytes();
            Array.Reverse(data);
            uint value = data.ToUInt32(0);
            return (value & 0xff000000) == 0x0a000000 || (value & 0xff000000) == 0x7f000000 || (value & 0xfff00000) == 0xac100000 || (value & 0xffff0000) == 0xc0a80000 || (value & 0xffff0000) == 0xa9fe0000;
        }

        /// <summary>
        /// 请求更多节点
        /// </summary>
        /// <param name="count"></param>
        protected abstract void NeedMorePeers(int count);

        protected override void OnReceive(object message)
        {
            switch (message)
            {
                case ChannelsConfig config:
                    OnStart(config);
                    break;
                case Timer _:
                    OnTimer();
                    break;
                case Peers peers:
                    AddPeers(peers.EndPoints);
                    break;
                case Connect connect:
                    ConnectToPeer(connect.EndPoint, connect.IsTrusted);
                    break;
                case WsConnected ws:
                    OnWsConnected(ws.Socket, ws.Remote, ws.Local);
                    break;
                case Tcp.Connected connected:
                    OnTcpConnected(((IPEndPoint)connected.RemoteAddress).Unmap(), ((IPEndPoint)connected.LocalAddress).Unmap());
                    break;
                case Tcp.Bound _:
                    tcp_listener = Sender;
                    break;
                case Tcp.CommandFailed commandFailed:
                    OnTcpCommandFailed(commandFailed.Cmd);
                    break;
                case Terminated terminated:
                    OnTerminated(terminated.ActorRef);
                    break;
            }
        }


        /// <summary>
        /// 这里是启动节点最开始的地方。
        /// 收到开始节点的消息，启动节点，设置TCP和Ws连接
        /// </summary>
        /// <param name="config"></param>
        private void OnStart(ChannelsConfig config)
        {
            ListenerTcpPort = config.Tcp?.Port ?? 0;
            ListenerWsPort = config.WebSocket?.Port ?? 0;

            MinDesiredConnections = config.MinDesiredConnections;
            MaxConnections = config.MaxConnections;
            MaxConnectionsPerAddress = config.MaxConnectionsPerAddress;
            //设置timer
            timer = Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(0, 5000, Context.Self, new Timer(), ActorRefs.NoSender);

            if ((ListenerTcpPort > 0 || ListenerWsPort > 0)
                && localAddresses.All(p => !p.IsIPv4MappedToIPv6 || IsIntranetAddress(p))
                && UPnP.Discover())
            {
                try
                {
                    localAddresses.Add(UPnP.GetExternalIP());

                    if (ListenerTcpPort > 0) UPnP.ForwardPort(ListenerTcpPort, ProtocolType.Tcp, "NEO Tcp");
                    if (ListenerWsPort > 0) UPnP.ForwardPort(ListenerWsPort, ProtocolType.Tcp, "NEO WebSocket");
                }
                catch { }
            }
            if (ListenerTcpPort > 0)
            {
                tcp_manager.Tell(new Tcp.Bind(Self, config.Tcp, options: new[] { new Inet.SO.ReuseAddress(true) }));
            }
            if (ListenerWsPort > 0)
            {
                var host = "*";

                if (!config.WebSocket.Address.GetAddressBytes().SequenceEqual(IPAddress.Any.GetAddressBytes()))
                {
                    // Is not for all interfaces
                    host = config.WebSocket.Address.ToString();
                }

                ws_host = new WebHostBuilder().UseKestrel().UseUrls($"http://{host}:{ListenerWsPort}").Configure(app => app.UseWebSockets().Run(ProcessWebSocketAsync)).Build();
                ws_host.Start();
            }
        }

        /// <summary>
        /// 收到TCP连接成功消息,更新ConnectingPeers和ConnectedPeers，ConnectedAddresses计数加一；
        /// 生成对应的RemoteNode和ProtocalHandle的子actor.
        /// </summary>
        /// <param name="remote"></param>
        /// <param name="local"></param>
        private void OnTcpConnected(IPEndPoint remote, IPEndPoint local)
        {
            ImmutableInterlocked.Update(ref ConnectingPeers, p => p.Remove(remote));
            if (MaxConnections != -1 && ConnectedPeers.Count >= MaxConnections && !TrustedIpAddresses.Contains(remote.Address))
            {
                Sender.Tell(Tcp.Abort.Instance);
                return;
            }
            //连接数超过最大值则终止连接
            ConnectedAddresses.TryGetValue(remote.Address, out int count);
            if (count >= MaxConnectionsPerAddress)
            {
                Sender.Tell(Tcp.Abort.Instance);
            }
            else
            {
                ConnectedAddresses[remote.Address] = count + 1;
                //对该连接生成对应的RemoteNode和ProtocalHandle的子actor
                IActorRef connection = Context.ActorOf(ProtocolProps(Sender, remote, local), $"connection_{Guid.NewGuid()}");
                Context.Watch(connection);
                Sender.Tell(new Tcp.Register(connection));
                ConnectedPeers.TryAdd(connection, remote);
            }
        }

        /// <summary>
        /// 收到Tcp连接失败消息，从ConnectingPeers删除对应节点
        /// </summary>
        /// <param name="cmd"></param>
        private void OnTcpCommandFailed(Tcp.Command cmd)
        {
            switch (cmd)
            {
                case Tcp.Connect connect:
                    ImmutableInterlocked.Update(ref ConnectingPeers, p => p.Remove(((IPEndPoint)connect.RemoteAddress).Unmap()));
                    break;
            }
        }

        /// <summary>
        /// 收到终止消息，尝试从ConnectedAddresses中将连接数减一，如果连接数为0则移除
        /// </summary>
        /// <param name="actorRef"></param>
        private void OnTerminated(IActorRef actorRef)
        {
            if (ConnectedPeers.TryRemove(actorRef, out IPEndPoint endPoint))
            {
                ConnectedAddresses.TryGetValue(endPoint.Address, out int count);
                if (count > 0) count--;
                if (count == 0)
                    ConnectedAddresses.Remove(endPoint.Address);
                else
                    ConnectedAddresses[endPoint.Address] = count;
            }
        }

        /// <summary>
        /// 收到timer消息，如果连接节点的数量大于等于最小要求，则返回；
        /// 如果UnconnectedPeers数量等于0，则请求更多节点信息；
        /// 否则从UnconnectedPeers中获取指定数量节点，建立连接。
        /// </summary>      
        private void OnTimer()
        {
            if (ConnectedPeers.Count >= MinDesiredConnections) return;
            if (UnconnectedPeers.Count == 0)
                NeedMorePeers(MinDesiredConnections - ConnectedPeers.Count);
            IPEndPoint[] endpoints = UnconnectedPeers.Take(MinDesiredConnections - ConnectedPeers.Count).ToArray();
            ImmutableInterlocked.Update(ref UnconnectedPeers, p => p.Except(endpoints));
            foreach (IPEndPoint endpoint in endpoints)
            {
                ConnectToPeer(endpoint);
            }
        }

        /// <summary>
        /// Ws连接消息
        /// </summary>
        /// <param name="ws"></param>
        /// <param name="remote"></param>
        /// <param name="local"></param>
        private void OnWsConnected(WebSocket ws, IPEndPoint remote, IPEndPoint local)
        {
            ConnectedAddresses.TryGetValue(remote.Address, out int count);
            if (count >= MaxConnectionsPerAddress)
            {
                ws.Abort();
            }
            else
            {
                ConnectedAddresses[remote.Address] = count + 1;
                Context.ActorOf(ProtocolProps(ws, remote, local), $"connection_{Guid.NewGuid()}");
            }
        }

        protected override void PostStop()
        {
            timer.CancelIfNotNull();
            ws_host?.Dispose();
            tcp_listener?.Tell(Tcp.Unbind.Instance);
            base.PostStop();
        }

        private async Task ProcessWebSocketAsync(HttpContext context)
        {
            if (!context.WebSockets.IsWebSocketRequest) return;
            WebSocket ws = await context.WebSockets.AcceptWebSocketAsync();
            Self.Tell(new WsConnected
            {
                Socket = ws,
                Remote = new IPEndPoint(context.Connection.RemoteIpAddress, context.Connection.RemotePort),
                Local = new IPEndPoint(context.Connection.LocalIpAddress, context.Connection.LocalPort)
            });
        }

        protected abstract Props ProtocolProps(object connection, IPEndPoint remote, IPEndPoint local);
    }
}

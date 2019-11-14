using Akka.Actor;
using Akka.Configuration;
using Akka.IO;
using Neo.Cryptography;
using Neo.IO;
using Neo.IO.Actors;
using Neo.Ledger;
using Neo.Network.P2P.Capabilities;
using Neo.Network.P2P.Payloads;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace Neo.Network.P2P
{
    public class RemoteNode : Connection
    {
        internal class Relay { public IInventory Inventory; }

        private readonly NeoSystem system;
        private readonly IActorRef protocol;
        private readonly Queue<Message> message_queue_high = new Queue<Message>();
        private readonly Queue<Message> message_queue_low = new Queue<Message>();
        private ByteString msg_buffer = ByteString.Empty;
        private BloomFilter bloom_filter;
        private bool ack = true;
        /// <summary>
        /// 已确认标记
        /// </summary>
        private bool verack = false;

        public IPEndPoint Listener => new IPEndPoint(Remote.Address, ListenerTcpPort);
        /// <summary>
        /// 连接方的TCP监听端口
        /// </summary>
        public int ListenerTcpPort { get; private set; } = 0;
        /// <summary>
        /// 对应连接的Version信息
        /// </summary>
        public VersionPayload Version { get; private set; }
        /// <summary>
        /// 连接方的最近区块高度
        /// </summary>
        public uint LastBlockIndex { get; private set; } = 0;
        /// <summary>
        /// 连接方是否为全节点
        /// </summary>
        public bool IsFullNode { get; private set; } = false;

        /// <summary>
        /// 构造函数
        /// 首先生成一个ProtocolHandler的子actor；
        /// 将自己加入LocalNode的RemoteNodes列表；
        /// 添加FullNodeCapability，如果有TCP和Ws监听端口，则添加对应的ServerCapability；
        /// 构造MessageCommand.Version并通过TCP或Ws发送出去。
        /// </summary>
        /// <param name="system"></param>
        /// <param name="connection"></param>
        /// <param name="remote"></param>
        /// <param name="local"></param>
        public RemoteNode(NeoSystem system, object connection, IPEndPoint remote, IPEndPoint local)
            : base(connection, remote, local)
        {
            this.system = system;
            this.protocol = Context.ActorOf(ProtocolHandler.Props(system));
            LocalNode.Singleton.RemoteNodes.TryAdd(Self, this);

            var capabilities = new List<NodeCapability>
            {
                new FullNodeCapability(Blockchain.Singleton.Height)
            };

            if (LocalNode.Singleton.ListenerTcpPort > 0) capabilities.Add(new ServerCapability(NodeCapabilityType.TcpServer, (ushort)LocalNode.Singleton.ListenerTcpPort));
            if (LocalNode.Singleton.ListenerWsPort > 0) capabilities.Add(new ServerCapability(NodeCapabilityType.WsServer, (ushort)LocalNode.Singleton.ListenerWsPort));

            SendMessage(Message.Create(MessageCommand.Version, VersionPayload.Create(LocalNode.Nonce, LocalNode.UserAgent, capabilities.ToArray())));
        }

        /// <summary>
        /// 检查消息队列，先放入高优先级消息，如果高优先级消息队列为空则放入低优先级消息；
        /// 然后发送消息
        /// </summary>
        private void CheckMessageQueue()
        {
            if (!verack || !ack) return;
            Queue<Message> queue = message_queue_high;
            if (queue.Count == 0)
            {
                queue = message_queue_low;
                if (queue.Count == 0) return;
            }
            SendMessage(queue.Dequeue());
        }

        /// <summary>
        /// 消息入队列
        /// </summary>
        /// <param name="command"></param>
        /// <param name="payload"></param>
        private void EnqueueMessage(MessageCommand command, ISerializable payload = null)
        {
            EnqueueMessage(Message.Create(command, payload));
        }

        /// <summary>
        /// 消息入队列，根据消息类型不同进入高优先级和低优先级
        /// </summary>
        /// <param name="message"></param>
        private void EnqueueMessage(Message message)
        {
            bool is_single = false;
            switch (message.Command)
            {
                case MessageCommand.Addr:
                case MessageCommand.GetAddr:
                case MessageCommand.GetBlocks:
                case MessageCommand.GetHeaders:
                case MessageCommand.Mempool:
                case MessageCommand.Ping:
                case MessageCommand.Pong:
                    is_single = true;
                    break;
            }
            Queue<Message> message_queue;
            switch (message.Command)
            {
                case MessageCommand.Alert:
                case MessageCommand.Consensus:
                case MessageCommand.FilterAdd:
                case MessageCommand.FilterClear:
                case MessageCommand.FilterLoad:
                case MessageCommand.GetAddr:
                case MessageCommand.Mempool:
                    message_queue = message_queue_high;
                    break;
                default:
                    message_queue = message_queue_low;
                    break;
            }
            if (!is_single || message_queue.All(p => p.Command != message.Command))
                message_queue.Enqueue(message);
            CheckMessageQueue();
        }

        protected override void OnAck()
        {
            ack = true;
            CheckMessageQueue();
        }

        /// <summary>
        /// 实现connection的抽象类，接收到TCP消息后的处理数据方法；
        /// 依次从msg_buffer中取出Message，然后交给ProtocalHandle处理。
        /// </summary>
        /// <param name="data">TCP接收到的ByteString</param>
        protected override void OnData(ByteString data)
        {
            msg_buffer = msg_buffer.Concat(data);

            for (Message message = TryParseMessage(); message != null; message = TryParseMessage())
                protocol.Tell(message);
        }

        /// <summary>
        /// 收到不同类型的消息处理方法
        /// </summary>
        /// <param name="message"></param>
        protected override void OnReceive(object message)
        {
            base.OnReceive(message);
            switch (message)
            {
                case Message msg:
                    EnqueueMessage(msg);
                    break;
                case IInventory inventory:
                    OnSend(inventory);
                    break;
                case Relay relay:
                    OnRelay(relay.Inventory);
                    break;
                case VersionPayload payload:
                    OnVersionPayload(payload);
                    break;
                case MessageCommand.Verack:
                    OnVerack();
                    break;
                case ProtocolHandler.SetFilter setFilter:
                    OnSetFilter(setFilter.Filter);
                    break;
                case PingPayload payload:
                    OnPingPayload(payload);
                    break;
            }
        }

        /// <summary>
        /// 收到PingPong消息则同步块高度
        /// </summary>
        /// <param name="payload"></param>
        private void OnPingPayload(PingPayload payload)
        {
            if (payload.LastBlockIndex > LastBlockIndex)
                LastBlockIndex = payload.LastBlockIndex;
        }

        /// <summary>
        /// 处理relay消息方法，对于tx消息如果有布隆过滤器则检测，其他进消息队列。
        /// </summary>
        /// <param name="inventory"></param>
        private void OnRelay(IInventory inventory)
        {
            if (!IsFullNode) return;
            if (inventory.InventoryType == InventoryType.TX)
            {
                if (bloom_filter != null && !bloom_filter.Test((Transaction)inventory))
                    return;
            }
            EnqueueMessage(MessageCommand.Inv, InvPayload.Create(inventory.InventoryType, inventory.Hash));
        }

        /// <summary>
        /// 处理inv消息方法，对于tx消息如果有布隆过滤器则检测，其他进消息队列。
        /// </summary>
        /// <param name="inventory"></param>
        private void OnSend(IInventory inventory)
        {
            if (!IsFullNode) return;
            if (inventory.InventoryType == InventoryType.TX)
            {
                if (bloom_filter != null && !bloom_filter.Test((Transaction)inventory))
                    return;
            }
            EnqueueMessage(inventory.InventoryType.ToMessageCommand(), inventory);
        }

        /// <summary>
        /// 设置布隆过滤器
        /// </summary>
        /// <param name="filter"></param>
        private void OnSetFilter(BloomFilter filter)
        {
            bloom_filter = filter;
        }

        /// <summary>
        /// 收到对方的verack消息，表示对方已确认连接；
        /// 将version发送给TaskManager
        /// </summary>
        private void OnVerack()
        {
            verack = true;
            system.TaskManager.Tell(new TaskManager.Register { Version = Version });
            CheckMessageQueue();
        }

        /// <summary>
        /// 收到连接方发来的Version负责信息，保存对方的相关信息；
        /// 如果没有重复连接，则最后发送Verack给对方。
        /// </summary>
        /// <param name="version"></param>
        private void OnVersionPayload(VersionPayload version)
        {
            Version = version;
            foreach (NodeCapability capability in version.Capabilities)
            {
                switch (capability)
                {
                    case FullNodeCapability fullNodeCapability:
                        IsFullNode = true;
                        LastBlockIndex = fullNodeCapability.StartHeight;
                        break;
                    case ServerCapability serverCapability:
                        if (serverCapability.Type == NodeCapabilityType.TcpServer)
                            ListenerTcpPort = serverCapability.Port;
                        break;
                }
            }
            if (version.Nonce == LocalNode.Nonce || version.Magic != ProtocolSettings.Default.Magic)
            {
                Disconnect(true);
                return;
            }
            if (LocalNode.Singleton.RemoteNodes.Values.Where(p => p != this).Any(p => p.Remote.Address.Equals(Remote.Address) && p.Version?.Nonce == version.Nonce))
            {
                Disconnect(true);
                return;
            }
            SendMessage(Message.Create(MessageCommand.Verack));
        }

        protected override void PostStop()
        {
            LocalNode.Singleton.RemoteNodes.TryRemove(Self, out _);
            base.PostStop();
        }

        internal static Props Props(NeoSystem system, object connection, IPEndPoint remote, IPEndPoint local)
        {
            return Akka.Actor.Props.Create(() => new RemoteNode(system, connection, remote, local)).WithMailbox("remote-node-mailbox");
        }

        /// <summary>
        /// 通过TCP发送消息
        /// </summary>
        /// <param name="message"></param>
        private void SendMessage(Message message)
        {
            ack = false;
            SendData(ByteString.FromBytes(message.ToArray()));
        }

        protected override SupervisorStrategy SupervisorStrategy()
        {
            return new OneForOneStrategy(ex =>
            {
                Disconnect(true);
                return Directive.Stop;
            }, loggingEnabled: false);
        }

        /// <summary>
        /// 从mag_buffer中解析出一个Message，并更新mag_buffer
        /// </summary>
        /// <returns></returns>
        private Message TryParseMessage()
        {
            var length = Message.TryDeserialize(msg_buffer, out var msg);
            if (length <= 0) return null;

            msg_buffer = msg_buffer.Slice(length).Compact();
            return msg;
        }
    }

    internal class RemoteNodeMailbox : PriorityMailbox
    {
        public RemoteNodeMailbox(Settings settings, Config config) : base(settings, config) { }

        internal protected override bool IsHighPriority(object message)
        {
            switch (message)
            {
                case Tcp.ConnectionClosed _:
                case Connection.Timer _:
                case Connection.Ack _:
                    return true;
                default:
                    return false;
            }
        }
    }
}

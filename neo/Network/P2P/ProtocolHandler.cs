using Akka.Actor;
using Akka.Configuration;
using Neo.Cryptography;
using Neo.IO;
using Neo.IO.Actors;
using Neo.IO.Caching;
using Neo.Ledger;
using Neo.Network.P2P.Payloads;
using Neo.Persistence;
using Neo.Plugins;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace Neo.Network.P2P
{
    internal class ProtocolHandler : UntypedActor
    {
        public class SetFilter { public BloomFilter Filter; }

        private readonly NeoSystem system;
        private readonly FIFOSet<UInt256> knownHashes;
        private readonly FIFOSet<UInt256> sentHashes;
        private VersionPayload version;
        private bool verack = false;
        private BloomFilter bloom_filter;

        public ProtocolHandler(NeoSystem system)
        {
            this.system = system;
            this.knownHashes = new FIFOSet<UInt256>(Blockchain.Singleton.MemPool.Capacity * 2);
            this.sentHashes = new FIFOSet<UInt256>(Blockchain.Singleton.MemPool.Capacity * 2);
        }

        /// <summary>
        /// 消息处理方法
        /// </summary>
        /// <param name="message"></param>
        protected override void OnReceive(object message)
        {
            if (!(message is Message msg)) return;
            foreach (IP2PPlugin plugin in Plugin.P2PPlugins)
                if (!plugin.OnP2PMessage(msg))
                    return;
            if (version == null)
            {
                if (msg.Command != MessageCommand.Version)
                    throw new ProtocolViolationException();
                OnVersionMessageReceived((VersionPayload)msg.Payload);
                return;
            }
            if (!verack)
            {
                if (msg.Command != MessageCommand.Verack)
                    throw new ProtocolViolationException();
                OnVerackMessageReceived();
                return;
            }
            switch (msg.Command)
            {
                case MessageCommand.Addr:
                    OnAddrMessageReceived((AddrPayload)msg.Payload);
                    break;
                case MessageCommand.Block:
                    OnInventoryReceived((Block)msg.Payload);
                    break;
                case MessageCommand.Consensus:
                    OnInventoryReceived((ConsensusPayload)msg.Payload);
                    break;
                case MessageCommand.FilterAdd:
                    OnFilterAddMessageReceived((FilterAddPayload)msg.Payload);
                    break;
                case MessageCommand.FilterClear:
                    OnFilterClearMessageReceived();
                    break;
                case MessageCommand.FilterLoad:
                    OnFilterLoadMessageReceived((FilterLoadPayload)msg.Payload);
                    break;
                case MessageCommand.GetAddr:
                    OnGetAddrMessageReceived();
                    break;
                case MessageCommand.GetBlocks:
                    OnGetBlocksMessageReceived((GetBlocksPayload)msg.Payload);
                    break;
                case MessageCommand.GetData:
                    OnGetDataMessageReceived((InvPayload)msg.Payload);
                    break;
                case MessageCommand.GetHeaders:
                    OnGetHeadersMessageReceived((GetBlocksPayload)msg.Payload);
                    break;
                case MessageCommand.Headers:
                    OnHeadersMessageReceived((HeadersPayload)msg.Payload);
                    break;
                case MessageCommand.Inv:
                    OnInvMessageReceived((InvPayload)msg.Payload);
                    break;
                case MessageCommand.Mempool:
                    OnMemPoolMessageReceived();
                    break;
                case MessageCommand.Ping:
                    OnPingMessageReceived((PingPayload)msg.Payload);
                    break;
                case MessageCommand.Pong:
                    OnPongMessageReceived((PingPayload)msg.Payload);
                    break;
                case MessageCommand.Transaction:
                    if (msg.Payload.Size <= Transaction.MaxTransactionSize)
                        OnInventoryReceived((Transaction)msg.Payload);
                    break;
                case MessageCommand.Verack:
                case MessageCommand.Version:
                    throw new ProtocolViolationException();
                case MessageCommand.Alert:
                case MessageCommand.MerkleBlock:
                case MessageCommand.NotFound:
                case MessageCommand.Reject:
                default: break;
            }
        }

        /// <summary>
        /// 接收到地址消息，向LocalNode发送消息，添加接收到的新节点。对应（GetAddr）消息。
        /// </summary>
        /// <param name="payload"></param>
        private void OnAddrMessageReceived(AddrPayload payload)
        {
            system.LocalNode.Tell(new Peer.Peers
            {
                EndPoints = payload.AddressList.Select(p => p.EndPoint).Where(p => p.Port > 0)
            });
        }

        /// <summary>
        /// 处理添加布隆过滤器消息
        /// </summary>
        /// <param name="payload"></param>
        private void OnFilterAddMessageReceived(FilterAddPayload payload)
        {
            if (bloom_filter != null)
                bloom_filter.Add(payload.Data);
        }

        /// <summary>
        /// 处理清除布隆过滤器消息
        /// </summary>
        private void OnFilterClearMessageReceived()
        {
            bloom_filter = null;
            Context.Parent.Tell(new SetFilter { Filter = null });
        }

        /// <summary>
        /// 收到加载布隆过滤器消息
        /// </summary>
        /// <param name="payload"></param>
        private void OnFilterLoadMessageReceived(FilterLoadPayload payload)
        {
            bloom_filter = new BloomFilter(payload.Filter.Length * 8, payload.K, payload.Tweak, payload.Filter);
            Context.Parent.Tell(new SetFilter { Filter = bloom_filter });
        }

        /// <summary>
        /// 收到获取地址消息,从本地的RemoteNodes中随机选取指定数量的节点信息，打包发送给RemoteNode处理
        /// </summary>
        private void OnGetAddrMessageReceived()
        {
            Random rand = new Random();
            IEnumerable<RemoteNode> peers = LocalNode.Singleton.RemoteNodes.Values
                .Where(p => p.ListenerTcpPort > 0)
                .GroupBy(p => p.Remote.Address, (k, g) => g.First())
                .OrderBy(p => rand.Next())
                .Take(AddrPayload.MaxCountToSend);
            NetworkAddressWithTime[] networkAddresses = peers.Select(p => NetworkAddressWithTime.Create(p.Listener.Address, p.Version.Timestamp, p.Version.Capabilities)).ToArray();
            if (networkAddresses.Length == 0) return;
            Context.Parent.Tell(Message.Create(MessageCommand.Addr, AddrPayload.Create(networkAddresses)));
        }

        /// <summary>
        /// 收到请求区块消息，从请求的hash开始发送指定数量的Block
        /// </summary>
        /// <param name="payload">请求区块消息，包括开始区块Hash以及请求块数</param>
        private void OnGetBlocksMessageReceived(GetBlocksPayload payload)
        {
            UInt256 hash = payload.HashStart;
            //payload.count未指定时为-1
            int count = payload.Count < 0 || payload.Count > InvPayload.MaxHashesCount ? InvPayload.MaxHashesCount : payload.Count;
            TrimmedBlock state = Blockchain.Singleton.Store.GetBlocks().TryGet(hash);
            if (state == null) return;
            List<UInt256> hashes = new List<UInt256>();
            for (uint i = 1; i <= count; i++)
            {
                uint index = state.Index + i;
                if (index > Blockchain.Singleton.Height)
                    break;
                hash = Blockchain.Singleton.GetBlockHash(index);
                if (hash == null) break;
                hashes.Add(hash);
            }
            if (hashes.Count == 0) return;
            Context.Parent.Tell(Message.Create(MessageCommand.Inv, InvPayload.Create(InventoryType.Block, hashes.ToArray())));
        }

        /// <summary>
        /// 获取数据消息；
        /// 如果是tx类型，获取hash对应的tx，返回给RemoteNode
        /// 如果是Block类型，没有布隆过滤器则获取对应block返回给RemoteNode,否则经布隆过滤器返回给RemoteNode.
        /// 如果是Consensus类型，当ConsensusRelayCache包含该hash时，取出对应inventoryConsensus返回给RemoteNode.
        /// </summary>
        /// <param name="payload">分为Tx，Block，Consensus</param>
        private void OnGetDataMessageReceived(InvPayload payload)
        {
            UInt256[] hashes = payload.Hashes.Where(p => sentHashes.Add(p)).ToArray();
            foreach (UInt256 hash in hashes)
            {
                switch (payload.Type)
                {
                    case InventoryType.TX:
                        Transaction tx = Blockchain.Singleton.GetTransaction(hash);
                        if (tx != null)
                            Context.Parent.Tell(Message.Create(MessageCommand.Transaction, tx));
                        break;
                    case InventoryType.Block:
                        Block block = Blockchain.Singleton.GetBlock(hash);
                        if (block != null)
                        {
                            if (bloom_filter == null)
                            {
                                Context.Parent.Tell(Message.Create(MessageCommand.Block, block));
                            }
                            else
                            {
                                BitArray flags = new BitArray(block.Transactions.Select(p => bloom_filter.Test(p)).ToArray());
                                Context.Parent.Tell(Message.Create(MessageCommand.MerkleBlock, MerkleBlockPayload.Create(block, flags)));
                            }
                        }
                        break;
                    case InventoryType.Consensus:
                        if (Blockchain.Singleton.ConsensusRelayCache.TryGet(hash, out IInventory inventoryConsensus))
                            Context.Parent.Tell(Message.Create(MessageCommand.Consensus, inventoryConsensus));
                        break;
                }
            }
        }

        /// <summary>
        /// 处理获取区块头请求，从当前区块链中获取指定数量区块头，打包发送回去。
        /// </summary>
        /// <param name="payload">请求区块头消息，包括开始区块Hash以及请求区块头数</param>
        private void OnGetHeadersMessageReceived(GetBlocksPayload payload)
        {
            UInt256 hash = payload.HashStart;
            int count = payload.Count < 0 || payload.Count > HeadersPayload.MaxHeadersCount ? HeadersPayload.MaxHeadersCount : payload.Count;
            DataCache<UInt256, TrimmedBlock> cache = Blockchain.Singleton.Store.GetBlocks();
            TrimmedBlock state = cache.TryGet(hash);
            if (state == null) return;
            List<Header> headers = new List<Header>();
            for (uint i = 1; i <= count; i++)
            {
                uint index = state.Index + i;
                hash = Blockchain.Singleton.GetBlockHash(index);
                if (hash == null) break;
                Header header = cache.TryGet(hash)?.Header;
                if (header == null) break;
                headers.Add(header);
            }
            if (headers.Count == 0) return;
            Context.Parent.Tell(Message.Create(MessageCommand.Headers, HeadersPayload.Create(headers)));
        }

        /// <summary>
        /// 收到区块头消息，交给BlockChain处理
        /// </summary>
        /// <param name="payload"></param>
        private void OnHeadersMessageReceived(HeadersPayload payload)
        {
            if (payload.Headers.Length == 0) return;
            system.Blockchain.Tell(payload.Headers, Context.Parent);
        }

        /// <summary>
        /// 收到返回的inventory消息（Block、Tx、Consensus），首先tell给TaskManager,再Tell给LocalNode作为Relay消息
        /// </summary>
        /// <param name="inventory"></param>
        private void OnInventoryReceived(IInventory inventory)
        {
            system.TaskManager.Tell(new TaskManager.TaskCompleted { Hash = inventory.Hash }, Context.Parent);
            system.LocalNode.Tell(new LocalNode.Relay { Inventory = inventory });
        }

        /// <summary>
        /// 收到Inv消息,针对不同的类型做不同的处理
        /// 对于本地未包含的区块和交易，交给TaskManager处理
        /// </summary>
        /// <param name="payload"></param>
        private void OnInvMessageReceived(InvPayload payload)
        {
            UInt256[] hashes = payload.Hashes.Where(p => knownHashes.Add(p) && !sentHashes.Contains(p)).ToArray();
            if (hashes.Length == 0) return;
            switch (payload.Type)
            {
                case InventoryType.Block:
                    using (Snapshot snapshot = Blockchain.Singleton.GetSnapshot())
                        hashes = hashes.Where(p => !snapshot.ContainsBlock(p)).ToArray();
                    break;
                case InventoryType.TX:
                    using (Snapshot snapshot = Blockchain.Singleton.GetSnapshot())
                        hashes = hashes.Where(p => !snapshot.ContainsTransaction(p)).ToArray();
                    break;
            }
            if (hashes.Length == 0) return;
            system.TaskManager.Tell(new TaskManager.NewTasks { Payload = InvPayload.Create(payload.Type, hashes) }, Context.Parent);
        }

        /// <summary>
        /// 对每个交易交给RemoteNode处理
        /// </summary>
        private void OnMemPoolMessageReceived()
        {
            foreach (InvPayload payload in InvPayload.CreateGroup(InventoryType.TX, Blockchain.Singleton.MemPool.GetVerifiedTransactions().Select(p => p.Hash).ToArray()))
                Context.Parent.Tell(Message.Create(MessageCommand.Inv, payload));
        }

        /// <summary>
        /// 收到Ping消息交给RemoteNode处理
        /// </summary>
        /// <param name="payload"></param>
        private void OnPingMessageReceived(PingPayload payload)
        {
            Context.Parent.Tell(payload);
            Context.Parent.Tell(Message.Create(MessageCommand.Pong, PingPayload.Create(Blockchain.Singleton.Height, payload.Nonce)));
        }

        /// <summary>
        /// 收到Pong消息交给RemoteNode处理
        /// </summary>
        /// <param name="payload"></param>
        private void OnPongMessageReceived(PingPayload payload)
        {
            Context.Parent.Tell(payload);
        }

        /// <summary>
        /// 收到verack消息交给RemoteNode处理
        /// </summary>
        private void OnVerackMessageReceived()
        {
            verack = true;
            Context.Parent.Tell(MessageCommand.Verack);
        }

        /// <summary>
        /// 收到version消息交给RemoteNode处理
        /// </summary>
        /// <param name="payload"></param>
        private void OnVersionMessageReceived(VersionPayload payload)
        {
            version = payload;
            Context.Parent.Tell(payload);
        }

        public static Props Props(NeoSystem system)
        {
            return Akka.Actor.Props.Create(() => new ProtocolHandler(system)).WithMailbox("protocol-handler-mailbox");
        }
    }

    internal class ProtocolHandlerMailbox : PriorityMailbox
    {
        public ProtocolHandlerMailbox(Settings settings, Config config)
            : base(settings, config)
        {
        }

        internal protected override bool IsHighPriority(object message)
        {
            if (!(message is Message msg)) return false;
            switch (msg.Command)
            {
                case MessageCommand.Consensus:
                case MessageCommand.FilterAdd:
                case MessageCommand.FilterClear:
                case MessageCommand.FilterLoad:
                case MessageCommand.Verack:
                case MessageCommand.Version:
                case MessageCommand.Alert:
                    return true;
                default:
                    return false;
            }
        }

        internal protected override bool ShallDrop(object message, IEnumerable queue)
        {
            if (!(message is Message msg)) return true;
            switch (msg.Command)
            {
                case MessageCommand.GetAddr:
                case MessageCommand.GetBlocks:
                case MessageCommand.GetData:
                case MessageCommand.GetHeaders:
                case MessageCommand.Mempool:
                    return queue.OfType<Message>().Any(p => p.Command == msg.Command);
                default:
                    return false;
            }
        }
    }
}

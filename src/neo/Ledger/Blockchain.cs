using Akka.Actor;
using Akka.Configuration;
using Neo.Cryptography.ECC;
using Neo.IO;
using Neo.IO.Actors;
using Neo.IO.Caching;
using Neo.Network.P2P;
using Neo.Network.P2P.Payloads;
using Neo.Persistence;
using Neo.Plugins;
using Neo.SmartContract;
using Neo.SmartContract.Native;
using Neo.VM;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Neo.Ledger
{
    public sealed partial class Blockchain : UntypedActor
    {
        public partial class ApplicationExecuted { }
        public class PersistCompleted { public Block Block; }
        public class Import { public IEnumerable<Block> Blocks; }
        public class ImportCompleted { }
        public class FillMemoryPool { public IEnumerable<Transaction> Transactions; }
        public class FillCompleted { }
        private class ParallelVerified { public Transaction Transaction; public bool ShouldRelay; public RelayResultReason VerifyResult; }

        public static readonly uint MillisecondsPerBlock = ProtocolSettings.Default.MillisecondsPerBlock;
        public const uint DecrementInterval = 2000000;
        public const int MaxValidators = 1024;
        public static readonly uint[] GenerationAmount = { 6, 5, 4, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
        public static readonly TimeSpan TimePerBlock = TimeSpan.FromMilliseconds(MillisecondsPerBlock);
        public static readonly ECPoint[] StandbyValidators = ProtocolSettings.Default.StandbyValidators.OfType<string>().Select(p => ECPoint.DecodePoint(p.HexToBytes(), ECCurve.Secp256r1)).ToArray();

        public static readonly Block GenesisBlock = new Block
        {
            PrevHash = UInt256.Zero,
            Timestamp = (new DateTime(2016, 7, 15, 15, 8, 21, DateTimeKind.Utc)).ToTimestampMS(),
            Index = 0,
            NextConsensus = GetConsensusAddress(StandbyValidators),
            Witness = new Witness
            {
                InvocationScript = Array.Empty<byte>(),
                VerificationScript = new[] { (byte)OpCode.PUSHT }
            },
            ConsensusData = new ConsensusData
            {
                PrimaryIndex = 0,
                Nonce = 2083236893
            },
            Transactions = new[] { DeployNativeContracts() }
        };

        private readonly static Script onPersistNativeContractScript;
        private const int MaxTxToReverifyPerIdle = 10;
        private static readonly object lockObj = new object();
        private readonly NeoSystem system;
        private readonly List<UInt256> header_index = new List<UInt256>();
        private uint stored_header_count = 0;
        private readonly Dictionary<uint, LinkedList<Block>> block_cache_unverified = new Dictionary<uint, LinkedList<Block>>();
        internal readonly RelayCache ConsensusRelayCache = new RelayCache(100);
        private SnapshotView currentSnapshot;

        public IStore Store { get; }
        public ReadOnlyView View { get; }
        public MemoryPool MemPool { get; }
        public uint Height => currentSnapshot.Height;
        public uint HeaderHeight => currentSnapshot.HeaderHeight;
        public UInt256 CurrentBlockHash => currentSnapshot.CurrentBlockHash;
        public UInt256 CurrentHeaderHash => currentSnapshot.CurrentHeaderHash;

        private static Blockchain singleton;
        public static Blockchain Singleton
        {
            get
            {
                while (singleton == null) Thread.Sleep(10);
                return singleton;
            }
        }

        static Blockchain()
        {
            GenesisBlock.RebuildMerkleRoot();

            NativeContract[] contracts = { NativeContract.GAS, NativeContract.NEO };
            using (ScriptBuilder sb = new ScriptBuilder())
            {
                foreach (NativeContract contract in contracts)
                    sb.EmitAppCall(contract.Hash, "onPersist");

                onPersistNativeContractScript = sb.ToArray();
            }
        }

        public Blockchain(NeoSystem system, IStore store)
        {
            this.system = system;
            this.MemPool = new MemoryPool(system, ProtocolSettings.Default.MemoryPoolMaxTransactions);
            this.Store = store;
            this.View = new ReadOnlyView(store);
            lock (lockObj)
            {
                if (singleton != null)
                    throw new InvalidOperationException();
                header_index.AddRange(View.HeaderHashList.Find().OrderBy(p => (uint)p.Key).SelectMany(p => p.Value.Hashes));
                stored_header_count += (uint)header_index.Count;
                if (stored_header_count == 0)
                {
                    header_index.AddRange(View.Blocks.Find().OrderBy(p => p.Value.Index).Select(p => p.Key));
                }
                else
                {
                    HashIndexState hashIndex = View.HeaderHashIndex.Get();
                    if (hashIndex.Index >= stored_header_count)
                    {
                        DataCache<UInt256, TrimmedBlock> cache = View.Blocks;
                        for (UInt256 hash = hashIndex.Hash; hash != header_index[(int)stored_header_count - 1];)
                        {
                            header_index.Insert((int)stored_header_count, hash);
                            hash = cache[hash].PrevHash;
                        }
                    }
                }
                if (header_index.Count == 0)
                {
                    Persist(GenesisBlock);
                }
                else
                {
                    UpdateCurrentSnapshot();
                    MemPool.LoadPolicy(currentSnapshot);
                }
                singleton = this;
            }
        }

        public bool ContainsBlock(UInt256 hash)
        {
            return View.ContainsBlock(hash);
        }

        public bool ContainsTransaction(UInt256 hash)
        {
            if (MemPool.ContainsKey(hash)) return true;
            return View.ContainsTransaction(hash);
        }

        private static Transaction DeployNativeContracts()
        {
            byte[] script;
            using (ScriptBuilder sb = new ScriptBuilder())
            {
                sb.EmitSysCall(InteropService.Neo_Native_Deploy);
                script = sb.ToArray();
            }
            return new Transaction
            {
                Version = 0,
                Script = script,
                Sender = (new[] { (byte)OpCode.PUSHT }).ToScriptHash(),
                SystemFee = 0,
                Attributes = new TransactionAttribute[0],
                Cosigners = new Cosigner[0],
                Witnesses = new[]
                {
                    new Witness
                    {
                        InvocationScript = Array.Empty<byte>(),
                        VerificationScript = new[] { (byte)OpCode.PUSHT }
                    }
                }
            };
        }

        public Block GetBlock(uint index)
        {
            if (index == 0) return GenesisBlock;
            UInt256 hash = GetBlockHash(index);
            if (hash == null) return null;
            return GetBlock(hash);
        }

        public Block GetBlock(UInt256 hash)
        {
            return View.GetBlock(hash);
        }

        public UInt256 GetBlockHash(uint index)
        {
            if (header_index.Count <= index) return null;
            return header_index[(int)index];
        }

        public static UInt160 GetConsensusAddress(ECPoint[] validators)
        {
            return Contract.CreateMultiSigRedeemScript(validators.Length - (validators.Length - 1) / 3, validators).ToScriptHash();
        }

        public Header GetHeader(uint index)
        {
            if (index == 0) return GenesisBlock.Header;
            UInt256 hash = GetBlockHash(index);
            if (hash == null) return null;
            return GetHeader(hash);
        }

        public Header GetHeader(UInt256 hash)
        {
            return View.GetHeader(hash);
        }

        public UInt256 GetNextBlockHash(UInt256 hash)
        {
            Header header = GetHeader(hash);
            if (header == null) return null;
            return GetBlockHash(header.Index + 1);
        }

        public SnapshotView GetSnapshot()
        {
            return new SnapshotView(Store);
        }

        public Transaction GetTransaction(UInt256 hash)
        {
            if (MemPool.TryGetValue(hash, out Transaction transaction))
                return transaction;
            return View.GetTransaction(hash);
        }

        private void OnImport(IEnumerable<Block> blocks)
        {
            foreach (Block block in blocks)
            {
                if (block.Index <= Height) continue;
                if (block.Index != Height + 1)
                    throw new InvalidOperationException();
                Persist(block);
                SaveHeaderHashList();
            }
            Sender.Tell(new ImportCompleted());
        }

        private void AddUnverifiedBlockToCache(Block block)
        {
            if (!block_cache_unverified.TryGetValue(block.Index, out LinkedList<Block> blocks))
            {
                blocks = new LinkedList<Block>();
                blocks.AddLast(block);
                block_cache_unverified.Add(block.Index, blocks);
            }
            else
            {
                foreach (var unverifiedBlock in blocks)
                {
                    if (block.Hash != unverifiedBlock.Hash)
                    {
                        blocks.AddLast(block);
                        return;
                    }
                }
            }
        }

        private void OnFillMemoryPool(IEnumerable<Transaction> transactions)
        {
            // Invalidate all the transactions in the memory pool, to avoid any failures when adding new transactions.
            MemPool.InvalidateAllTransactions();

            // Add the transactions to the memory pool
            foreach (var tx in transactions)
            {
                if (View.ContainsTransaction(tx.Hash))
                    continue;
                if (!NativeContract.Policy.CheckPolicy(tx, currentSnapshot))
                    continue;
                // First remove the tx if it is unverified in the pool.
                MemPool.TryRemoveUnVerified(tx.Hash, out _);
                // Verify the the transaction
                if (tx.Verify(currentSnapshot, MemPool.SendersFeeMonitor.GetSenderFee(tx.Sender)) != RelayResultReason.Succeed)
                    continue;
                // Add to the memory pool
                MemPool.TryAdd(tx.Hash, tx);
            }
            // Transactions originally in the pool will automatically be reverified based on their priority.

            Sender.Tell(new FillCompleted());
        }

        private RelayResultReason OnNewBlock(Block block)
        {
            if (block.Index <= Height)
                return RelayResultReason.AlreadyExists;
            if (block.Index - 1 > Height)
            {
                AddUnverifiedBlockToCache(block);
                return RelayResultReason.UnableToVerify;
            }
            if (block.Index == Height + 1)
            {
                if (!block.Verify(currentSnapshot))
                {
                    system.SyncManager.Tell(new SyncManager.InvalidBlockIndex { InvalidIndex = block.Index });
                    return RelayResultReason.Invalid;
                } 
                block_cache_unverified.Remove(block.Index);
                Persist(block);
                system.LocalNode.Tell(new LocalNode.RelayDirectly { Inventory = block });
                system.SyncManager.Tell(new SyncManager.PersistedBlockIndex { PersistedIndex = block.Index });
                SaveHeaderHashList();
                if (block_cache_unverified.TryGetValue(Height + 1, out LinkedList<Block> unverifiedBlocks))
                {
                    foreach (var unverifiedBlock in unverifiedBlocks)
                        Self.Tell(unverifiedBlock, ActorRefs.NoSender);
                    block_cache_unverified.Remove(Height + 1);
                }
            }
            return RelayResultReason.Succeed;
        }

        private RelayResultReason OnNewConsensus(ConsensusPayload payload)
        {
            if (!payload.Verify(currentSnapshot)) return RelayResultReason.Invalid;
            system.Consensus?.Tell(payload);
            ConsensusRelayCache.Add(payload);
            system.LocalNode.Tell(new LocalNode.RelayDirectly { Inventory = payload });
            return RelayResultReason.Succeed;
        }

        private void OnNewHeaders(Header[] headers)
        {
            using (SnapshotView snapshot = GetSnapshot())
            {
                foreach (Header header in headers)
                {
                    if (header.Index - 1 >= header_index.Count) break;
                    if (header.Index < header_index.Count) continue;
                    if (!header.Verify(snapshot)) break;
                    header_index.Add(header.Hash);
                    snapshot.Blocks.Add(header.Hash, header.Trim());
                    snapshot.HeaderHashIndex.GetAndChange().Hash = header.Hash;
                    snapshot.HeaderHashIndex.GetAndChange().Index = header.Index;
                }
                SaveHeaderHashList(snapshot);
                snapshot.Commit();
            }
            UpdateCurrentSnapshot();
            system.TaskManager.Tell(new TaskManager.HeaderTaskCompleted(), Sender);
        }

        private void OnNewTransaction(Transaction transaction, bool relay)
        {
            RelayResultReason reason;
            if (ContainsTransaction(transaction.Hash))
                reason = RelayResultReason.AlreadyExists;
            else if (!MemPool.CanTransactionFitInPool(transaction))
                reason = RelayResultReason.OutOfMemory;
            else
                reason = transaction.VerifyForEachBlock(currentSnapshot, MemPool.SendersFeeMonitor.GetSenderFee(transaction.Sender));
            if (reason == RelayResultReason.Succeed)
            {
                Task.Run(() =>
                {
                    return new ParallelVerified
                    {
                        Transaction = transaction,
                        ShouldRelay = relay,
                        VerifyResult = transaction.VerifyParallelParts(currentSnapshot)
                    };
                }).PipeTo(Self, Sender);
            }
            else
            {
                Sender.Tell(reason);
            }
        }

        private void OnParallelVerified(ParallelVerified parallelVerified)
        {
            RelayResultReason reason = parallelVerified.VerifyResult;
            if (reason == RelayResultReason.Succeed)
            {
                if (!MemPool.CanTransactionFitInPool(parallelVerified.Transaction))
                    reason = RelayResultReason.OutOfMemory;
                else if (!MemPool.TryAdd(parallelVerified.Transaction.Hash, parallelVerified.Transaction))
                    reason = RelayResultReason.OutOfMemory;
                else if (parallelVerified.ShouldRelay)
                    system.LocalNode.Tell(new LocalNode.RelayDirectly { Inventory = parallelVerified.Transaction });
            }
            Sender.Tell(reason);
        }

        private void OnPersistCompleted(Block block)
        {
            MemPool.UpdatePoolForBlockPersisted(block, currentSnapshot);
            Context.System.EventStream.Publish(new PersistCompleted { Block = block });
        }

        protected override void OnReceive(object message)
        {
            switch (message)
            {
                case Import import:
                    OnImport(import.Blocks);
                    break;
                case FillMemoryPool fill:
                    OnFillMemoryPool(fill.Transactions);
                    break;
                case Header[] headers:
                    OnNewHeaders(headers);
                    break;
                case Block block:
                    Sender.Tell(OnNewBlock(block));
                    break;
                case Transaction[] transactions:
                    {
                        // This message comes from a mempool's revalidation, already relayed
                        foreach (var tx in transactions) OnNewTransaction(tx, false);
                        break;
                    }
                case Transaction transaction:
                    OnNewTransaction(transaction, true);
                    break;
                case ParallelVerified parallelVerified:
                    OnParallelVerified(parallelVerified);
                    break;
                case ConsensusPayload payload:
                    Sender.Tell(OnNewConsensus(payload));
                    break;
                case Idle _:
                    if (MemPool.ReVerifyTopUnverifiedTransactionsIfNeeded(MaxTxToReverifyPerIdle, currentSnapshot))
                        Self.Tell(Idle.Instance, ActorRefs.NoSender);
                    break;
            }
        }

        private void Persist(Block block)
        {
            using (SnapshotView snapshot = GetSnapshot())
            {
                List<ApplicationExecuted> all_application_executed = new List<ApplicationExecuted>();
                snapshot.PersistingBlock = block;
                if (block.Index > 0)
                {
                    using (ApplicationEngine engine = new ApplicationEngine(TriggerType.System, null, snapshot, 0, true))
                    {
                        engine.LoadScript(onPersistNativeContractScript);
                        if (engine.Execute() != VMState.HALT) throw new InvalidOperationException();
                        ApplicationExecuted application_executed = new ApplicationExecuted(engine);
                        Context.System.EventStream.Publish(application_executed);
                        all_application_executed.Add(application_executed);
                    }
                }
                snapshot.Blocks.Add(block.Hash, block.Trim());
                foreach (Transaction tx in block.Transactions)
                {
                    var state = new TransactionState
                    {
                        BlockIndex = block.Index,
                        Transaction = tx
                    };

                    snapshot.Transactions.Add(tx.Hash, state);

                    using (ApplicationEngine engine = new ApplicationEngine(TriggerType.Application, tx, snapshot.Clone(), tx.SystemFee))
                    {
                        engine.LoadScript(tx.Script);
                        state.VMState = engine.Execute();
                        if (state.VMState == VMState.HALT)
                        {
                            engine.Snapshot.Commit();
                        }
                        ApplicationExecuted application_executed = new ApplicationExecuted(engine);
                        Context.System.EventStream.Publish(application_executed);
                        all_application_executed.Add(application_executed);
                    }
                }
                snapshot.BlockHashIndex.GetAndChange().Set(block);
                if (block.Index == header_index.Count)
                {
                    header_index.Add(block.Hash);
                    snapshot.HeaderHashIndex.GetAndChange().Set(block);
                }
                foreach (IPersistencePlugin plugin in Plugin.PersistencePlugins)
                    plugin.OnPersist(snapshot, all_application_executed);
                snapshot.Commit();
                List<Exception> commitExceptions = null;
                foreach (IPersistencePlugin plugin in Plugin.PersistencePlugins)
                {
                    try
                    {
                        plugin.OnCommit(snapshot);
                    }
                    catch (Exception ex)
                    {
                        if (plugin.ShouldThrowExceptionFromCommit(ex))
                        {
                            if (commitExceptions == null)
                                commitExceptions = new List<Exception>();

                            commitExceptions.Add(ex);
                        }
                    }
                }
                if (commitExceptions != null) throw new AggregateException(commitExceptions);
            }
            UpdateCurrentSnapshot();
            OnPersistCompleted(block);
        }

        protected override void PostStop()
        {
            base.PostStop();
            currentSnapshot?.Dispose();
        }

        public static Props Props(NeoSystem system, IStore store)
        {
            return Akka.Actor.Props.Create(() => new Blockchain(system, store)).WithMailbox("blockchain-mailbox");
        }

        private void SaveHeaderHashList(SnapshotView snapshot = null)
        {
            if ((header_index.Count - stored_header_count < 2000))
                return;
            bool snapshot_created = snapshot == null;
            if (snapshot_created) snapshot = GetSnapshot();
            try
            {
                while (header_index.Count - stored_header_count >= 2000)
                {
                    snapshot.HeaderHashList.Add(stored_header_count, new HeaderHashList
                    {
                        Hashes = header_index.Skip((int)stored_header_count).Take(2000).ToArray()
                    });
                    stored_header_count += 2000;
                }
                if (snapshot_created) snapshot.Commit();
            }
            finally
            {
                if (snapshot_created) snapshot.Dispose();
            }
        }

        private void UpdateCurrentSnapshot()
        {
            Interlocked.Exchange(ref currentSnapshot, GetSnapshot())?.Dispose();
        }
    }

    internal class BlockchainMailbox : PriorityMailbox
    {
        public BlockchainMailbox(Akka.Actor.Settings settings, Config config)
            : base(settings, config)
        {
        }

        internal protected override bool IsHighPriority(object message)
        {
            switch (message)
            {
                case Header[] _:
                case Block _:
                case ConsensusPayload _:
                case Terminated _:
                    return true;
                default:
                    return false;
            }
        }
    }
}

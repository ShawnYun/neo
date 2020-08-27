using Akka.Actor;
using Akka.Configuration;
using Neo.IO.Actors;
using Neo.IO.Caching;
using Neo.Ledger;
using Neo.Network.P2P.Payloads;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Neo.Network.P2P
{
    internal class TaskManager : UntypedActor
    {
        public class Register { public VersionPayload Version; }
        public class Update { public uint LastBlockIndex; public bool RequestTasks; }
        public class NewTasks { public InvPayload Payload; }
        public class RestartTasks { public InvPayload Payload; }
        private class Timer { }

        private static readonly TimeSpan TimerInterval = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan TaskTimeout = TimeSpan.FromMinutes(1);
        private static readonly UInt256 MemPoolTaskHash = UInt256.Parse("0x0000000000000000000000000000000000000000000000000000000000000001");

        private const int MaxConncurrentTasks = 3;
        private const int MaxSyncTasksCount = 500;
        private const int PingCoolingOffPeriod = 60_000; // in ms.

        private readonly NeoSystem system;
        /// <summary>
        /// A set of known hashes, of inventories or payloads, already received.
        /// </summary>
        private readonly HashSetCache<UInt256> knownHashes;
        private readonly Dictionary<UInt256, int> globalTasks = new Dictionary<UInt256, int>();
        private readonly Dictionary<uint, TaskSession> receivedBlockIndex = new Dictionary<uint, TaskSession>();
        private readonly HashSet<uint> failedSyncTasks = new HashSet<uint>();
        private readonly Dictionary<IActorRef, TaskSession> sessions = new Dictionary<IActorRef, TaskSession>();
        private readonly ICancelable timer = Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(TimerInterval, TimerInterval, Context.Self, new Timer(), ActorRefs.NoSender);
        private uint lastTaskIndex = 0;

        public TaskManager(NeoSystem system)
        {
            this.system = system;
            this.knownHashes = new HashSetCache<UInt256>(Blockchain.Singleton.MemPool.Capacity * 2 / 5);
            this.lastTaskIndex = Blockchain.Singleton.Height;
            Context.System.EventStream.Subscribe(Self, typeof(Blockchain.PersistCompleted));
            Context.System.EventStream.Subscribe(Self, typeof(Blockchain.RelayResult));
        }

        private bool AssignSyncTask(uint index, TaskSession filterSession = null)
        {
            if (index <= Blockchain.Singleton.Height || sessions.Values.Any(p => p != filterSession && p.IndexTasks.ContainsKey(index)))
                return true;
            KeyValuePair<IActorRef, TaskSession> remoteNode = sessions.Where(p => p.Value != filterSession && p.Value.LastBlockIndex >= index)
                .OrderBy(p => p.Value.Weight)
                .FirstOrDefault();
            if (remoteNode.Value == null)
            {
                failedSyncTasks.Add(index);
                return false;
            }
            TaskSession session = remoteNode.Value;
            session.IndexTasks.TryAdd(index, TimeProvider.Current.UtcNow);
            session.UpdateWeight();
            remoteNode.Key.Tell(Message.Create(MessageCommand.GetBlockByIndex, GetBlockByIndexPayload.Create(index, 1)));
            failedSyncTasks.Remove(index);
            return true;
        }

        private void OnBlock(Block block)
        {
            var session = sessions.Values.FirstOrDefault(p => p.IndexTasks.ContainsKey(block.Index));
            if (session is null) return;
            var newRTT = (TimeProvider.Current.UtcNow - session.IndexTasks[block.Index]).TotalMilliseconds;
            session.UpdateRTT(newRTT);
            session.IndexTasks.Remove(block.Index);
            session.UpdateWeight();
            receivedBlockIndex.TryAdd(block.Index, session);
            RequestTasks();
        }

        private void OnInvalidBlock(Block invalidBlock)
        {
            if (!receivedBlockIndex.TryGetValue(invalidBlock.Index, out TaskSession session))
            {
                AssignSyncTask(invalidBlock.Index);
                return;
            }
            receivedBlockIndex.Remove(invalidBlock.Index);
            var actor = sessions.Where(p => p.Value == session).FirstOrDefault().Key;
            if (actor != null)
            {
                OnTerminated(actor);
                system.LocalNode.Tell(new LocalNode.MaliciousNode { actor = actor });
                AssignSyncTask(invalidBlock.Index);
            }
        }

        private void OnValidBlock(Block validBlock)
        {
            if (receivedBlockIndex.TryGetValue(validBlock.Index, out TaskSession session) && session.MaxTaskCountPerNode < 50)
                session.MaxTaskCountPerNode++;
        }

        private void OnNewTasks(InvPayload payload)
        {
            if (!sessions.TryGetValue(Sender, out TaskSession session))
                return;
            // Do not accept payload of type InventoryType.TX if not synced on best known HeaderHeight
            if (payload.Type == InventoryType.TX && Blockchain.Singleton.Height < sessions.Values.Max(p => p.LastBlockIndex))
                return;
            HashSet<UInt256> hashes = new HashSet<UInt256>(payload.Hashes);
            // Remove all previously processed knownHashes from the list that is being requested
            hashes.Remove(knownHashes);

            // Remove those that are already in process by other sessions
            hashes.Remove(globalTasks);
            if (hashes.Count == 0)
                return;

            // Update globalTasks with the ones that will be requested within this current session
            foreach (UInt256 hash in hashes)
            {
                IncrementGlobalTask(hash);
                session.InvTasks[hash] = DateTime.UtcNow;
            }

            foreach (InvPayload group in InvPayload.CreateGroup(payload.Type, hashes.ToArray()))
                Sender.Tell(Message.Create(MessageCommand.GetData, group));
        }

        private void OnPersistCompleted(Block block)
        {
            receivedBlockIndex.Remove(block.Index);
        }

        protected override void OnReceive(object message)
        {
            switch (message)
            {
                case Register register:
                    OnRegister(register.Version);
                    break;
                case Update update:
                    OnUpdate(update);
                    break;
                case NewTasks tasks:
                    OnNewTasks(tasks.Payload);
                    break;
                case RestartTasks restart:
                    OnRestartTasks(restart.Payload);
                    break;
                case IInventory inventory:
                    OnTaskCompleted(inventory);
                    break;
                case Blockchain.PersistCompleted pc:
                    OnPersistCompleted(pc.Block);
                    break;
                case Blockchain.RelayResult rr:
                    if (rr.Inventory is Block invalidBlock && rr.Result == VerifyResult.Invalid)
                        OnInvalidBlock(invalidBlock);
                    else if (rr.Inventory is Block validBlock && rr.Result == VerifyResult.Succeed)
                        OnValidBlock(validBlock);
                    break;
                case Timer _:
                    OnTimer();
                    break;
                case Terminated terminated:
                    OnTerminated(terminated.ActorRef);
                    break;
            }
        }

        private void OnRegister(VersionPayload version)
        {
            Context.Watch(Sender);
            TaskSession session = new TaskSession(version);
            if (session.IsFullNode)
                session.InvTasks.TryAdd(MemPoolTaskHash, TimeProvider.Current.UtcNow);
            sessions.TryAdd(Sender, session);
            RequestTasks();
        }

        private void OnUpdate(Update update)
        {
            if (!sessions.TryGetValue(Sender, out TaskSession session))
                return;
            session.LastBlockIndex = update.LastBlockIndex;
            session.ExpireTime = TimeProvider.Current.UtcNow.AddMilliseconds(PingCoolingOffPeriod);
            if (update.RequestTasks) RequestTasks();
        }

        private void OnRestartTasks(InvPayload payload)
        {
            knownHashes.ExceptWith(payload.Hashes);
            foreach (UInt256 hash in payload.Hashes)
                globalTasks.Remove(hash);
            foreach (InvPayload group in InvPayload.CreateGroup(payload.Type, payload.Hashes))
                system.LocalNode.Tell(Message.Create(MessageCommand.GetData, group));
        }

        private void OnTaskCompleted(IInventory inventory)
        {
            if (inventory is Block block)
                OnBlock(block);
            var hash = inventory.Hash;
            knownHashes.Add(hash);
            globalTasks.Remove(hash);
            if (sessions.TryGetValue(Sender, out TaskSession session))
                session.InvTasks.Remove(hash);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DecrementGlobalTask(UInt256 hash)
        {
            if (globalTasks.TryGetValue(hash, out var value))
            {
                if (value == 1)
                    globalTasks.Remove(hash);
                else
                    globalTasks[hash] = value - 1;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IncrementGlobalTask(UInt256 hash)
        {
            if (!globalTasks.TryGetValue(hash, out var value))
            {
                globalTasks[hash] = 1;
                return true;
            }
            if (value >= MaxConncurrentTasks)
                return false;

            globalTasks[hash] = value + 1;
            return true;
        }

        private void OnTerminated(IActorRef actor)
        {
            if (!sessions.TryGetValue(actor, out TaskSession session))
                return;
            foreach (uint index in session.IndexTasks.Keys)
                AssignSyncTask(index, session);

            foreach (UInt256 hash in session.InvTasks.Keys)
                DecrementGlobalTask(hash);
            sessions.Remove(actor);
        }

        private void OnTimer()
        {
            foreach (TaskSession session in sessions.Values)
            {
                foreach (KeyValuePair<uint, DateTime> kvp in session.IndexTasks)
                {
                    if (TimeProvider.Current.UtcNow - kvp.Value > TaskTimeout)
                    {
                        session.IndexTasks.Remove(kvp.Key);
                        if (++session.TimeoutTimes > 3)
                            OnTerminated(sessions.Where(p => p.Value == session).First().Key);
                        else
                        {
                            session.UpdateWeight();
                            AssignSyncTask(kvp.Key, session);
                        }
                    }
                }

                foreach (var task in session.InvTasks.ToArray())
                {
                    if (DateTime.UtcNow - task.Value > TaskTimeout)
                    {
                        if (session.InvTasks.Remove(task.Key))
                            DecrementGlobalTask(task.Key);
                    }
                }
            }
            RequestTasks();
        }

        protected override void PostStop()
        {
            timer.CancelIfNotNull();
            base.PostStop();
        }

        public static Props Props(NeoSystem system)
        {
            return Akka.Actor.Props.Create(() => new TaskManager(system)).WithMailbox("task-manager-mailbox");
        }

        private void RequestTasks()
        {
            if (sessions.Count() == 0) return;

            SendPingMessage();

            int taskCounts = sessions.Values.Sum(p => p.IndexTasks.Count);

            while (failedSyncTasks.Count() > 0)
            {
                if (taskCounts >= MaxSyncTasksCount) return;
                var failedTask = failedSyncTasks.OrderBy(p => p).First();
                if (failedTask <= Blockchain.Singleton.Height)
                {
                    failedSyncTasks.Remove(failedTask);
                    continue;
                }
                if (AssignSyncTask(failedTask))
                    taskCounts++;
                else
                    return;
            }

            var highestBlockIndex = sessions.Values.Max(p => p.LastBlockIndex);
            for (; taskCounts < MaxSyncTasksCount; taskCounts++)
            {
                if (lastTaskIndex >= highestBlockIndex || lastTaskIndex > Blockchain.Singleton.HeaderHeight + 2000) break;
                if (!AssignSyncTask(++lastTaskIndex)) break;
            }
        }

        private void SendPingMessage()
        {
            foreach (KeyValuePair<IActorRef, TaskSession> item in sessions)
            {
                var node = item.Key;
                var session = item.Value;

                if (session.ExpireTime < TimeProvider.Current.UtcNow ||
                     (Blockchain.Singleton.Height >= session.LastBlockIndex
                     && TimeProvider.Current.UtcNow.ToTimestampMS() - PingCoolingOffPeriod >= Blockchain.Singleton.GetBlock(Blockchain.Singleton.CurrentBlockHash)?.Timestamp))
                {
                    if (session.InvTasks.Remove(MemPoolTaskHash))
                    {
                        node.Tell(Message.Create(MessageCommand.Mempool));
                    }
                    node.Tell(Message.Create(MessageCommand.Ping, PingPayload.Create(Blockchain.Singleton.Height)));
                    session.ExpireTime = TimeProvider.Current.UtcNow.AddMilliseconds(PingCoolingOffPeriod);
                }
            }
        }
    }

    internal class TaskManagerMailbox : PriorityMailbox
    {
        public TaskManagerMailbox(Akka.Actor.Settings settings, Config config)
            : base(settings, config)
        {
        }

        internal protected override bool IsHighPriority(object message)
        {
            switch (message)
            {
                case TaskManager.Register _:
                case TaskManager.RestartTasks _:
                    return true;
                case TaskManager.NewTasks tasks:
                    if (tasks.Payload.Type == InventoryType.Block || tasks.Payload.Type == InventoryType.Consensus)
                        return true;
                    return false;
                default:
                    return false;
            }
        }

        internal protected override bool ShallDrop(object message, IEnumerable queue)
        {
            if (!(message is TaskManager.NewTasks tasks)) return false;
            // Remove duplicate tasks
            if (queue.OfType<TaskManager.NewTasks>().Any(x => x.Payload.Type == tasks.Payload.Type && x.Payload.Hashes.SequenceEqual(tasks.Payload.Hashes))) return true;
            return false;
        }
    }
}

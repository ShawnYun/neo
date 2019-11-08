using Akka.Actor;
using Neo.Network.P2P.Capabilities;
using Neo.Network.P2P.Payloads;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Neo.Network.P2P
{
    internal class TaskSession
    {
        public readonly IActorRef RemoteNode;
        public readonly VersionPayload Version;
        public readonly Dictionary<UInt256, DateTime> Tasks = new Dictionary<UInt256, DateTime>();
        /// <summary>
        /// Pending high priority tasks of type `InventoryType.Block`
        /// </summary>
        public readonly HashSet<UInt256> InvBlockPendingTasks = new HashSet<UInt256>();

        public bool HasTask => Tasks.Count > 0;
        public uint StartHeight { get; }

        public TaskSession(IActorRef node, VersionPayload version)
        {
            this.RemoteNode = node;
            this.Version = version;
            this.StartHeight = version.Capabilities
                .OfType<FullNodeCapability>()
                .FirstOrDefault()?.StartHeight ?? 0;
        }
    }
}

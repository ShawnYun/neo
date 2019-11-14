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
        /// <summary>
        /// 对应的RemoteNode的actor引用
        /// </summary>
        public readonly IActorRef RemoteNode;
        /// <summary>
        /// 对应的RemoteNode的Version信息
        /// </summary>
        public readonly VersionPayload Version;

        public readonly Dictionary<UInt256, DateTime> Tasks = new Dictionary<UInt256, DateTime>();
        public readonly HashSet<UInt256> AvailableTasks = new HashSet<UInt256>();

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

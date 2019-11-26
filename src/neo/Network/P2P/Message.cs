using Akka.IO;
using Neo.Cryptography;
using Neo.IO;
using Neo.IO.Caching;
using System;
using System.IO;

namespace Neo.Network.P2P
{
    /// <summary>
    /// P2P消息类
    /// </summary>
    public class Message : ISerializable
    {
        /// <summary>
        /// 负载最大尺寸
        /// </summary>
        public const int PayloadMaxSize = 0x02000000;
        /// <summary>
        /// 压缩最小尺寸
        /// </summary>
        private const int CompressionMinSize = 128;
        /// <summary>
        /// 压缩阈值
        /// </summary>
        private const int CompressionThreshold = 64;
        /// <summary>
        /// 消息标记位，0表示未压缩，1表示压缩
        /// </summary>
        public MessageFlags Flags;
        /// <summary>
        /// 消息指令，包括握手，连接，同步，SPV等指令
        /// </summary>
        public MessageCommand Command;
        /// <summary>
        /// 消息负载
        /// </summary>
        public ISerializable Payload;
        /// <summary>
        /// 压缩的负载
        /// </summary>
        private byte[] _payload_compressed;

        public int Size => sizeof(MessageFlags) + sizeof(MessageCommand) + _payload_compressed.GetVarSize();

        /// <summary>
        /// 创建消息并尝试压缩，如果经过压缩后长度缩减大于阈值，则将消息变为压缩消息。
        /// </summary>
        /// <param name="command">消息指令</param>
        /// <param name="payload">消息负载</param>
        /// <returns></returns>
        public static Message Create(MessageCommand command, ISerializable payload = null)
        {
            Message message = new Message
            {
                Flags = MessageFlags.None,
                Command = command,
                Payload = payload,
                _payload_compressed = payload?.ToArray() ?? new byte[0]
            };

            // Try compression
            if (message._payload_compressed.Length > CompressionMinSize)
            {
                var compressed = message._payload_compressed.CompressLz4();
                if (compressed.Length < message._payload_compressed.Length - CompressionThreshold)
                {
                    message._payload_compressed = compressed;
                    message.Flags |= MessageFlags.Compressed;
                }
            }

            return message;
        }


        /// <summary>
        /// 解压缩Payload，并根据不同的指令类型做对应的序列化处理
        /// </summary>
        private void DecompressPayload()
        {
            if (_payload_compressed.Length == 0) return;
            byte[] decompressed = Flags.HasFlag(MessageFlags.Compressed)
                ? _payload_compressed.DecompressLz4(PayloadMaxSize)
                : _payload_compressed;
            Payload = ReflectionCache<MessageCommand>.CreateSerializable(Command, decompressed);
        }

        /// <summary>
        /// 反序列化
        /// </summary>
        /// <param name="reader"></param>
        void ISerializable.Deserialize(BinaryReader reader)
        {
            Flags = (MessageFlags)reader.ReadByte();
            Command = (MessageCommand)reader.ReadByte();
            _payload_compressed = reader.ReadVarBytes(PayloadMaxSize);
            DecompressPayload();
        }

        /// <summary>
        /// 序列化
        /// </summary>
        /// <param name="writer"></param>
        void ISerializable.Serialize(BinaryWriter writer)
        {
            writer.Write((byte)Flags);
            writer.Write((byte)Command);
            writer.WriteVarBytes(_payload_compressed);
        }

        internal static int TryDeserialize(ByteString data, out Message msg)
        {
            msg = null;
            if (data.Count < 3) return 0;

            var header = data.Slice(0, 3).ToArray();
            var flags = (MessageFlags)header[0];
            ulong length = header[2];
            var payloadIndex = 3;

            if (length == 0xFD)
            {
                if (data.Count < 5) return 0;
                length = data.Slice(payloadIndex, 2).ToArray().ToUInt16(0);
                payloadIndex += 2;
            }
            else if (length == 0xFE)
            {
                if (data.Count < 7) return 0;
                length = data.Slice(payloadIndex, 4).ToArray().ToUInt32(0);
                payloadIndex += 4;
            }
            else if (length == 0xFF)
            {
                if (data.Count < 11) return 0;
                length = data.Slice(payloadIndex, 8).ToArray().ToUInt64(0);
                payloadIndex += 8;
            }

            if (length > PayloadMaxSize) throw new FormatException();

            if (data.Count < (int)length + payloadIndex) return 0;

            msg = new Message()
            {
                Flags = flags,
                Command = (MessageCommand)header[1],
                _payload_compressed = length <= 0 ? new byte[0] : data.Slice(payloadIndex, (int)length).ToArray()
            };
            msg.DecompressPayload();

            return payloadIndex + (int)length;
        }
    }
}

using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace KafkaClone.Shared;

public class Broker
{
    public int Id { get; set; }
    public int Port { get; set; }        // TCP port (data plane - for clients)
    public int GrpcPort { get; set; }    // gRPC port (control plane - for Raft)
    public string Host { get; set; }

    // Primary constructor with both ports
    public Broker(int id, int port, int grpcPort, string host)
    {
        Id = id;
        Port = port;
        GrpcPort = grpcPort;
        Host = host;
    }

    // Backward compatibility constructor (assumes gRPC port = TCP port - 3000)
    public Broker(int id, int port, string host)
    {
        Id = id;
        Port = port;
        GrpcPort = port - 3000; // Convention: 9092 → 6092
        Host = host;
    }

    // Serialization format:
    // Id (4 bytes)
    // HostLength (2 bytes)
    // Host (N bytes)
    // Port (4 bytes)
    // GrpcPort (4 bytes)
    public byte[] Serialize()
    {
        byte[] hostBytes = Encoding.UTF8.GetBytes(Host);

        int totalLength = 4 + 2 + hostBytes.Length + 4 + 4; // Id + len + host + Port + GrpcPort
        byte[] buffer = new byte[totalLength];

        int offset = 0;

        // 1. Id
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(offset), Id);
        offset += 4;

        // 2. Host length
        BinaryPrimitives.WriteInt16LittleEndian(buffer.AsSpan(offset), (short)hostBytes.Length);
        offset += 2;

        // 3. Host bytes
        hostBytes.CopyTo(buffer, offset);
        offset += hostBytes.Length;

        // 4. Port (TCP)
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(offset), Port);
        offset += 4;

        // 5. GrpcPort
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(offset), GrpcPort);

        return buffer;
    }

    public static async Task<Broker> DeserializeAsync(Stream stream)
    {
        // Id (4 bytes)
        byte[] idBytes = new byte[4];
        await stream.ReadExactlyAsync(idBytes, 0, 4);
        int id = BitConverter.ToInt32(idBytes);

        // HostLength (2 bytes)
        byte[] hostLenBytes = new byte[2];
        await stream.ReadExactlyAsync(hostLenBytes, 0, 2);
        short hostLen = BitConverter.ToInt16(hostLenBytes);

        // Host (N bytes)
        byte[] hostBytes = new byte[hostLen];
        await stream.ReadExactlyAsync(hostBytes, 0, hostLen);
        string host = Encoding.UTF8.GetString(hostBytes);

        // Port (4 bytes - TCP)
        byte[] portBytes = new byte[4];
        await stream.ReadExactlyAsync(portBytes, 0, 4);
        int port = BitConverter.ToInt32(portBytes);

        // GrpcPort (4 bytes)
        byte[] grpcPortBytes = new byte[4];
        await stream.ReadExactlyAsync(grpcPortBytes, 0, 4);
        int grpcPort = BitConverter.ToInt32(grpcPortBytes);

        return new Broker(id, port, grpcPort, host);
    }

    public override string ToString()
    {
        return $"Broker {Id} at {Host}:{Port} (gRPC: {GrpcPort})";
    }
}
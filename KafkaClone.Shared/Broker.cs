using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;


namespace KafkaClone.Shared;

public class Broker
{

    public int Id { get; set; }

    public int Port { get; set; }

    public string Host { get; set; }

    public Broker(int id,int port,string host)
    {
        Id = id;
        Port = port;
        Host = host;
    }
        // Id (4 bytes)
        // HostLength (2 bytes)
        // Host (N bytes)
        // Port (4 bytes)
    public byte[] Serialize()
    {
        byte[] hostBytes = Encoding.UTF8.GetBytes(Host);

        int totalLength = 4 + 2 + hostBytes.Length + 4; // Id + len + host + Port
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

        // 4. Port
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(offset), Port);

        return buffer;
    }


    public static async Task<Broker> DeserializeAsync(Stream stream)
    {
        //Id (4 bytes)
        byte[] idBytes = new byte[4];
        await stream.ReadExactlyAsync(idBytes,0,4);
        int id = BitConverter.ToInt32(idBytes);

        // HostLength (2 bytes)
        byte[] hostLenBytes = new byte[2];
        await stream.ReadExactlyAsync(hostLenBytes,0,2);
        short hostLen = BitConverter.ToInt16(hostLenBytes);

        // Host (N bytes)
        byte[] hostBytes = new byte[hostLen];
        await stream.ReadExactlyAsync(hostBytes,0,hostLen);
        string host = Encoding.UTF8.GetString(hostBytes);

        // Port (4 bytes)
        byte[] portBytes = new byte[4];
        await stream.ReadExactlyAsync(portBytes,0,4);

        int port = BitConverter.ToInt32(portBytes);


        return new Broker(id,port,host);
    }

    public override string ToString()
    {
        return $"Broker {Id} at {Host}:{Port}";
    }

}
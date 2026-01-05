using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using KafkaClone.Service;
using KafkaClone.Shared;

namespace KafkaClone.Server;

public class TcpServerService : BackgroundService
{
    private readonly BrokerService _brokerService;
    private readonly Broker _myIdentity;
    private readonly ILogger<TcpServerService> _logger;

    public TcpServerService(
        BrokerService brokerService, 
        Broker myIdentity, 
        ILogger<TcpServerService> logger)
    {
        _brokerService = brokerService;
        _myIdentity = myIdentity;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        TcpListener listener = new TcpListener(IPAddress.Any, _myIdentity.Port);
        listener.Start();
        
        _logger.LogInformation($"[TCP] Data Plane listening on port {_myIdentity.Port}...");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                TcpClient client = await listener.AcceptTcpClientAsync(stoppingToken);
                // Fire-and-forget to handle multiple clients concurrently
                _ = HandleClientAsync(client, stoppingToken);
            }
        }
        catch (OperationCanceledException) { /* Graceful shutdown */ }
        finally
        {
            listener.Stop();
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken ct)
    {
        using (client)
        using (NetworkStream stream = client.GetStream())
        {
            byte[] cmdBuffer = new byte[1];

            while (!ct.IsCancellationRequested && client.Connected)
            {
                int bytesRead = await stream.ReadAsync(cmdBuffer, ct);
                if (bytesRead == 0) break;

                try
                {
                    switch (cmdBuffer[0])
                    {
                        case 0: // PRODUCE
                            await HandleProduce(stream, ct);
                            break;
                        case 1: // CONSUME
                            await HandleConsume(stream, ct);
                            break;
                        case 2: // BATCH PRODUCE
                            await HandleBatchProduce(stream, ct);
                            break;
                        case 3: // BATCH CONSUME
                            await HandleBatchConsume(stream, ct);
                            break;
                        default:
                            _logger.LogWarning($"Unknown Command: {cmdBuffer[0]}");
                            break;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Client Error: {ex.Message}");
                    break; 
                }
            }
        }
    }

    // =========================================================
    //  DATA PLANE HANDLERS
    // =========================================================

    private async Task HandleProduce(NetworkStream stream, CancellationToken ct)
    {
        // --------------- PRODUCE --------------------
        //   Protocol: |1|len|topic|4|PartitionId||4|valueSize|value
        // --------------------------------------------

        // 1. Read Topic
        byte[] topicLenBuf = new byte[2];
        await stream.ReadExactlyAsync(topicLenBuf, 0, 2);
        short topicLen = BitConverter.ToInt16(topicLenBuf);

        byte[] topicBuf = new byte[topicLen];
        await stream.ReadExactlyAsync(topicBuf, 0, topicLen);
        string topicName = Encoding.UTF8.GetString(topicBuf);

        // 3. Read PartitionId
        byte[] partIdBytes = new byte[4];
        await stream.ReadExactlyAsync(partIdBytes,0,4);
        int partitionId = BitConverter.ToInt32(partIdBytes);

        // 3. Read Payload Size
        byte[] sizeBuf = new byte[4];
        await stream.ReadExactlyAsync(sizeBuf, 0, 4);
        int size = BitConverter.ToInt32(sizeBuf);

        // 4. Read Payload
        byte[] payload = new byte[size];
        await stream.ReadExactlyAsync(payload, 0, size);

        // 5. Save
        await _brokerService.ProducAsync(topicName,partitionId,payload);
                                
        _logger.LogDebug($"[Produce] Written {size} bytes to {topicName}-{partitionId}");
    }

private async Task HandleBatchProduce(NetworkStream stream, CancellationToken ct)
{
         // --------------------------- BATCH PRODUCE ---------------------------
        // Protocol: |5|len|topic|4|PartitionId|4|batchsize|len_msg1|msg1|len_msg2|mesg2|...
        // --------------------------------------------------------------------

        // 1. Read Topic
        byte[] topicLenBuf = new byte[2];
        await stream.ReadExactlyAsync(topicLenBuf, 0, 2);
        short topicLen = BitConverter.ToInt16(topicLenBuf);

        byte[] topicBuf = new byte[topicLen];
        await stream.ReadExactlyAsync(topicBuf, 0, topicLen);
        string topicName = Encoding.UTF8.GetString(topicBuf);

        // 2. Read PartitionId
        byte[] partIdBytes = new byte[4];
        await stream.ReadExactlyAsync(partIdBytes,0,4);
        int partitionId = BitConverter.ToInt32(partIdBytes);

        // 3
        byte[] msgCountBuf = new byte[4];
        await stream.ReadExactlyAsync(msgCountBuf, 0, 4);
        int msgCount = BitConverter.ToInt32(msgCountBuf);

        List<byte[]> cache = new List<byte[]>();

        for (int i = 0; i < msgCount; i++)
        {
            // 3. Read Payload Size
            byte[] sizeBuf = new byte[4];
            await stream.ReadExactlyAsync(sizeBuf, 0, 4);
            int size = BitConverter.ToInt32(sizeBuf);

            // 4. Read Payload
            byte[] payload = new byte[size];
            await stream.ReadExactlyAsync(payload, 0, size);

            cache.Add(payload);
        }
        
        await _brokerService.BatchProduceAsync(topicName,partitionId,cache);
        
    
    _logger.LogDebug($"[BatchProduce] Written {msgCount} messages to {topicName}-{partitionId}");
}

    private async Task HandleConsume(NetworkStream stream, CancellationToken ct)
    {
       // ----------------- CONSUME -----------------
        // Protocol: |2|len|topic|4|partId|8|offset
        // -------------------------------------------

        // 1. Read Topic
        byte[] topicLenBuf = new byte[2];
        await stream.ReadExactlyAsync(topicLenBuf, 0, 2);
        short topicLen = BitConverter.ToInt16(topicLenBuf);

        byte[] topicBuf = new byte[topicLen];
        await stream.ReadExactlyAsync(topicBuf, 0, topicLen);
        string topicName = Encoding.UTF8.GetString(topicBuf);

        // 2. Read Partition ID
        byte[] partIdBuf = new byte[4];
        await stream.ReadExactlyAsync(partIdBuf, 0, 4);
        int partitionId = BitConverter.ToInt32(partIdBuf);

        // 3. Read Offset
        byte[] offsetBuf = new byte[8];
        await stream.ReadExactlyAsync(offsetBuf, 0, 8);
        long offset = BitConverter.ToInt64(offsetBuf);

        try
        {
            var result = await _brokerService.ConsumeAsync(topicName,partitionId,offset);
            
            // Found it! Send: [NextOffset(8)] [Size(4)] [Data]
            byte[] sizeBytes = BitConverter.GetBytes(result.Length);
            long nextOffset = offset + 1;

            await stream.WriteAsync(BitConverter.GetBytes(nextOffset));
            await stream.WriteAsync(sizeBytes);
            await stream.WriteAsync(result);
            Console.WriteLine($"[Consume] Sent offset {offset} from '{topicName}-{partitionId}'");
        }
        catch (IndexOutOfRangeException)
        {
            // End of Log. Send -1.
            await stream.WriteAsync(BitConverter.GetBytes((long)-1));
        }
    }

    private async Task HandleBatchConsume(NetworkStream stream, CancellationToken ct)
    {
        // --------------------------- BATCH CONSUME ---------------------------
        // receive: |6|len(2)|topic|partId(4)|offset(8)|batchsize(4)|
        // return: |nextOffset||batchsize|len_msg1|msg1|len_msg2|mesg2|...
        // --------------------------------------------------------------------

        // 1. Read Topic
        byte[] topicLenBuf = new byte[2];
        await stream.ReadExactlyAsync(topicLenBuf, 0, 2);
        short topicLen = BitConverter.ToInt16(topicLenBuf);

        byte[] topicBuf = new byte[topicLen];
        await stream.ReadExactlyAsync(topicBuf, 0, topicLen);
        string topicName = Encoding.UTF8.GetString(topicBuf);

        // 2. Read Partition ID
        byte[] partIdBuf = new byte[4];
        await stream.ReadExactlyAsync(partIdBuf, 0, 4);
        int partitionId = BitConverter.ToInt32(partIdBuf);

        // 4. get starting offset
        byte[] offsetBytes = new byte[8];
        await stream.ReadExactlyAsync(offsetBytes,0,8);
        long offset = BitConverter.ToInt64(offsetBytes);

        // 5. Get batch request size
        byte[] msgCountBuf = new byte[4];
        await stream.ReadExactlyAsync(msgCountBuf, 0, 4);
        int msgCount = BitConverter.ToInt32(msgCountBuf);


        // 6. Get all messages
        var result = await _brokerService.BatchConsumeAsync(topicName, partitionId,offset,msgCount);

        byte[] nextOffsetBytes = BitConverter.GetBytes(result.nextOffset);

        // 7. Return: |lastOffset|msgCount|len_msg1|msg1|len_msg2|mesg2|...

        byte[] returnMsgsCount = BitConverter.GetBytes(result.messages.Count());

        var bufferedStream = new BufferedStream(stream, 8192); // 8KB buffer


        await bufferedStream.WriteAsync(nextOffsetBytes);
        await bufferedStream.WriteAsync(returnMsgsCount);
        
        foreach(var msg in result.messages)
        {
            byte[] msgLength = BitConverter.GetBytes(msg.Length);

            await bufferedStream.WriteAsync(msgLength);
            await bufferedStream.WriteAsync(msg);
        }

        await bufferedStream.FlushAsync();
    }
}
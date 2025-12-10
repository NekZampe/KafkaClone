using System.Net;
using System.Net.Sockets;
using System.Text;
using KafkaClone.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

namespace KafkaClone;

class Program
{
    static async Task Main(string[] args)
    {
        string basePath = Path.Combine(Directory.GetCurrentDirectory(), "kafka-data");

        // 1. Setup the Logger Factory
        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
            builder.SetMinimumLevel(LogLevel.Information);
        });

        // 2. Initialize Managers
        TopicManager topicManager = new TopicManager(basePath, loggerFactory);
        OffsetManager offsetManager = new OffsetManager(basePath);

        // 3. Start the Server
        TcpListener listener = new TcpListener(IPAddress.Any, 9092);
        listener.Start();
        Console.WriteLine("Kafka Clone listening on port 9092...");

        // 4. Main Accept Loop
        while (true)
        {
            TcpClient client = await listener.AcceptTcpClientAsync();
            Console.WriteLine("Client connected!");

            // Fire and forget (don't await the task itself so we can accept the next client)
            _ = HandleClientAsync(client, topicManager, offsetManager);
        }
    }

    static async Task HandleClientAsync(TcpClient client, TopicManager topicManager, OffsetManager offsetManager)
    {
        // "using" ensures the client is closed when this method finishes
        using (client)
        using (NetworkStream stream = client.GetStream())
        {
            byte[] cmdBuffer = new byte[1];

            while (true)
            {
                // Read the Command Byte
                int bytesRead = await stream.ReadAsync(cmdBuffer);

                // Check for disconnect
                if (bytesRead == 0)
                {
                    Console.WriteLine("Client disconnected.");
                    break;
                }

                try
                {
                    switch (cmdBuffer[0])
                    {
                        case 0:
                            {
                                // ----------- CREATE TOPIC (NEW) ----------
                                // Protocol: |0|len|topic|4|partitionCount|
                                // -----------------------------------------

                                // 1. Read Topic
                                byte[] topicLenBuf = new byte[2];
                                await stream.ReadExactlyAsync(topicLenBuf, 0, 2);
                                short topicLen = BitConverter.ToInt16(topicLenBuf);

                                byte[] topicBuf = new byte[topicLen];
                                await stream.ReadExactlyAsync(topicBuf, 0, topicLen);
                                string topicName = Encoding.UTF8.GetString(topicBuf);

                                // 2. Read Count
                                byte[] countBuf = new byte[4];
                                await stream.ReadExactlyAsync(countBuf, 0, 4);
                                int count = BitConverter.ToInt32(countBuf);

                                // 3. Execute
                                topicManager.CreateTopic(topicName, count);

                                // 4. Send Ack (1 = Success)
                                await stream.WriteAsync(new byte[] { 1 });
                                break;
                            }

                        case 1:
                            {
                                // ----------- PRODUCE -------------
                                // Protocol: |1|len|topic|4|valueSize|value
                                // ---------------------------------

                                // 1. Read Topic
                                byte[] topicLenBuf = new byte[2];
                                await stream.ReadExactlyAsync(topicLenBuf, 0, 2);
                                short topicLen = BitConverter.ToInt16(topicLenBuf);

                                byte[] topicBuf = new byte[topicLen];
                                await stream.ReadExactlyAsync(topicBuf, 0, topicLen);
                                string topicName = Encoding.UTF8.GetString(topicBuf);

                                // 2. Get Partition (Round Robin)
                                Partition partition = topicManager.GetTopic(topicName);

                                // 3. Read Payload Size
                                byte[] sizeBuf = new byte[4];
                                await stream.ReadExactlyAsync(sizeBuf, 0, 4);
                                int size = BitConverter.ToInt32(sizeBuf);

                                // 4. Read Payload
                                byte[] payload = new byte[size];
                                await stream.ReadExactlyAsync(payload, 0, size);

                                // 5. Save
                                await partition.AppendAsync(payload);
                                Console.WriteLine($"[Produce] Saved {size} bytes to '{topicName}'");
                                break;
                            }

                        case 2:
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

                                // 3. Get Specific Partition
                                Partition partition = topicManager.GetTopic(topicName, partitionId);

                                // 4. Read Offset
                                byte[] offsetBuf = new byte[8];
                                await stream.ReadExactlyAsync(offsetBuf, 0, 8);
                                long offset = BitConverter.ToInt64(offsetBuf);

                                try
                                {
                                    byte[] messageData = await partition.ReadAsync(offset);

                                    // Found it! Send: [NextOffset(8)] [Size(4)] [Data]
                                    byte[] sizeBytes = BitConverter.GetBytes(messageData.Length);
                                    long nextOffset = offset + 1;

                                    await stream.WriteAsync(BitConverter.GetBytes(nextOffset));
                                    await stream.WriteAsync(sizeBytes);
                                    await stream.WriteAsync(messageData);
                                    Console.WriteLine($"[Consume] Sent offset {offset} from '{topicName}-{partitionId}'");
                                }
                                catch (IndexOutOfRangeException)
                                {
                                    // End of Log. Send -1.
                                    await stream.WriteAsync(BitConverter.GetBytes((long)-1));
                                }
                                break;
                            }

                        case 3:
                            {
                                // --------------------- COMMIT --------------------------
                                // Protocol: |3|len|group|len|topic|4|partId|8|offset
                                // --------------------------------------------------------

                                // 1. Read Group
                                byte[] groupLenBuf = new byte[2];
                                await stream.ReadExactlyAsync(groupLenBuf, 0, 2);
                                short groupLen = BitConverter.ToInt16(groupLenBuf);
                                byte[] groupBuf = new byte[groupLen];
                                await stream.ReadExactlyAsync(groupBuf, 0, groupLen);
                                string group = Encoding.UTF8.GetString(groupBuf);

                                // 2. Read Topic
                                byte[] topicLenBuf = new byte[2];
                                await stream.ReadExactlyAsync(topicLenBuf, 0, 2);
                                short topicLen = BitConverter.ToInt16(topicLenBuf);
                                byte[] topicBuf = new byte[topicLen];
                                await stream.ReadExactlyAsync(topicBuf, 0, topicLen);
                                string topic = Encoding.UTF8.GetString(topicBuf);

                                // 3. Read Partition ID
                                byte[] partIdBuf = new byte[4];
                                await stream.ReadExactlyAsync(partIdBuf, 0, 4);
                                int partitionId = BitConverter.ToInt32(partIdBuf);

                                // 4. Read Offset
                                byte[] offsetBuf = new byte[8];
                                await stream.ReadExactlyAsync(offsetBuf, 0, 8);
                                long offsetToCommit = BitConverter.ToInt64(offsetBuf);

                                // 5. Save
                                await offsetManager.CommitOffset(group, topic, partitionId, offsetToCommit);
                                Console.WriteLine($"[Commit] Group '{group}' on '{topic}-{partitionId}' @ {offsetToCommit}");
                                break;
                            }

                        case 4:
                            {
                                // ------------- FETCH OFFSET -------------
                                // Protocol: |4|len|group|len|topic|4|partId|
                                // ----------------------------------------

                                // 1. Read Group
                                byte[] groupLenBuf = new byte[2];
                                await stream.ReadExactlyAsync(groupLenBuf, 0, 2);
                                short groupLen = BitConverter.ToInt16(groupLenBuf);
                                byte[] groupBuf = new byte[groupLen];
                                await stream.ReadExactlyAsync(groupBuf, 0, groupLen);
                                string group = Encoding.UTF8.GetString(groupBuf);

                                // 2. Read Topic
                                byte[] topicLenBuf = new byte[2];
                                await stream.ReadExactlyAsync(topicLenBuf, 0, 2);
                                short topicLen = BitConverter.ToInt16(topicLenBuf);
                                byte[] topicBuf = new byte[topicLen];
                                await stream.ReadExactlyAsync(topicBuf, 0, topicLen);
                                string topic = Encoding.UTF8.GetString(topicBuf);

                                // 3. Read Partition ID
                                byte[] partIdBuf = new byte[4];
                                await stream.ReadExactlyAsync(partIdBuf, 0, 4);
                                int partitionId = BitConverter.ToInt32(partIdBuf);

                                // 4. Retrieve
                                long storedOffset = offsetManager.GetOffset(group, topic, partitionId);

                                // 5. Send back
                                await stream.WriteAsync(BitConverter.GetBytes(storedOffset));
                                Console.WriteLine($"[Fetch] Group '{group}' on '{topic}-{partitionId}' is @ {storedOffset}");
                                break;
                            }
                        case 5:
                            {
                                // --------------------------- BATCH PRODUCE ---------------------------
                                // Protocol: |5|len|topic|4|batchsize|len_msg1|msg1|len_msg2|mesg2|...
                                // --------------------------------------------------------------------

                                // 1. Read Topic
                                byte[] topicLenBuf = new byte[2];
                                await stream.ReadExactlyAsync(topicLenBuf, 0, 2);
                                short topicLen = BitConverter.ToInt16(topicLenBuf);

                                byte[] topicBuf = new byte[topicLen];
                                await stream.ReadExactlyAsync(topicBuf, 0, topicLen);
                                string topicName = Encoding.UTF8.GetString(topicBuf);

                                // 2. Get Partition (Round Robin)
                                Partition partition = topicManager.GetTopic(topicName);

                                //New
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

                                await partition.AppendBatchAsync(cache);

                                Console.WriteLine($"[BatchProduce] Saved {msgCount} messages to '{topicName}'");

                                break;
                            }
                            case 6:
                            {
                                // --------------------------- BATCH READ ---------------------------
                                // Protocol: |6|len|topic|4|partId|4|batchsize|len_msg1|msg1|len_msg2|mesg2|...
                                // --------------------------------------------------------------------

                                break;
                            }

                        default:
                            Console.WriteLine($"Unknown Command: {cmdBuffer[0]}");
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error handling client request: {ex.Message}");
                    break; // Close connection on error
                }
            }
        }
    }
}
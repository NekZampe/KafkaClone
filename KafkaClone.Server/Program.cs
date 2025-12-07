using System.Net;
using System.Net.Sockets;
using System.Text;
using KafkaClone.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;


string basePath = Path.Combine(Directory.GetCurrentDirectory(), "kafka-data");

// 1. Setup the Logger Factory
using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
{
    // This tells it to print to the screen
    builder.AddConsole(); 
    
    // Optional: Set minimum level (Info, Warning, Error)
    builder.SetMinimumLevel(LogLevel.Information); 
});

TopicManager topicManager = new TopicManager(basePath,loggerFactory);

// NEW
OffsetManager offsetManager = new OffsetManager(basePath);

// 1. Listen on Any IP, Port 9092
TcpListener listener = new TcpListener(IPAddress.Any, 9092);

// 2. Start the server
listener.Start();
Console.WriteLine("Kafka Clone listening on port 9092...");

// 3. Keep the app running
while (true)
{
   TcpClient client = await listener.AcceptTcpClientAsync();
    Console.WriteLine("Client connected!");
    _ = HandleClientAsync(client, topicManager, offsetManager);
}



static async Task HandleClientAsync(TcpClient client,TopicManager topicManager, OffsetManager offsetManager)
{
  using (client) 
    using (NetworkStream stream = client.GetStream())
    {
    byte[] cmdBuffer = new byte[1];

    while (true)
        {

    int bytesRead = await stream.ReadAsync(cmdBuffer);

    // 1. Check for disconnect first
    if (bytesRead == 0)
    {
        Console.WriteLine("Client disconnected");
        return;
    }

    // 2. Switch on the command byte
    switch (cmdBuffer[0])
    {
        case 1:
        {
            // 1. Get topic size
            byte[] topicSizeBuffer = new byte[2];
            await stream.ReadExactlyAsync(topicSizeBuffer,0,2);
            short topicLength = BitConverter.ToInt16(topicSizeBuffer);
            // 2. Read topic data and get Topic from TopicManager
            byte[] topicBuffer = new byte[topicLength];
            await stream.ReadExactlyAsync(topicBuffer,0,topicLength);
            string topicName = Encoding.UTF8.GetString(topicBuffer);

            Partition partition = topicManager.GetTopic(topicName);

            //Same as before
            byte[] sizeBuffer = new byte[4];
            await stream.ReadExactlyAsync(sizeBuffer,0,4);
            int size = BitConverter.ToInt32(sizeBuffer,0);
            byte[] payload = new byte[size];
            await stream.ReadExactlyAsync(payload,0,size);
            await partition.AppendAsync(payload);
            Console.WriteLine($"Saved {size} bytes to disk.");
            break;
        }
        case 2:
        {
            
            // 1. Read Topic
            byte[] topicSizeBuffer = new byte[2];
            await stream.ReadExactlyAsync(topicSizeBuffer, 0, 2);
            short topicLength = BitConverter.ToInt16(topicSizeBuffer);
            
            byte[] topicBuffer = new byte[topicLength];
            await stream.ReadExactlyAsync(topicBuffer, 0, topicLength);
            string topicName = Encoding.UTF8.GetString(topicBuffer);

            Partition partition = topicManager.GetTopic(topicName);

            // 2. Read Offset
            byte[] offsetBytes = new byte[8];
            await stream.ReadExactlyAsync(offsetBytes, 0, 8);
            long offset = BitConverter.ToInt64(offsetBytes, 0);

            // 3. TRY to read. Let the Partition decide if it exists!
            try 
            {
                byte[] messageData = await partition.ReadAsync(offset);
                
                // If we are here, we found it! Send it back.
                byte[] sizeBytes = BitConverter.GetBytes(messageData.Length);
                long nextOffset = offset + 1;
                
                await stream.WriteAsync(BitConverter.GetBytes(nextOffset));
                await stream.WriteAsync(sizeBytes);
                await stream.WriteAsync(messageData);
            }
            catch (IndexOutOfRangeException) // Catch the specific error from ReadAsync
            {
                // Message not found (End of Log)
                long endOfFileSignal = -1;
                await stream.WriteAsync(BitConverter.GetBytes(endOfFileSignal));
            }
            
            break;
        }
        case 3:
        {
            Console.WriteLine("COMMIT Request");

            // 1. Read Group Name (2 byte length prefix)
            byte[] groupSizeBuffer =  new byte[2];
            await stream.ReadExactlyAsync(groupSizeBuffer,0,2);
            short groupLen = BitConverter.ToInt16(groupSizeBuffer, 0);

            byte[] groupBuffer = new byte[groupLen];
            await stream.ReadExactlyAsync(groupBuffer, 0, groupLen);
            string group = Encoding.UTF8.GetString(groupBuffer);

            // 2. Read Topic Name (2 byte length prefix)
            byte[] topicLenBuffer = new byte[2];
            await stream.ReadExactlyAsync(topicLenBuffer, 0, 2);
            short topicLen = BitConverter.ToInt16(topicLenBuffer, 0);

            byte[] topicBuffer = new byte[topicLen];
            await stream.ReadExactlyAsync(topicBuffer, 0, topicLen);
            string topic = Encoding.UTF8.GetString(topicBuffer);

            // 3. Read Offset (8 bytes)
            byte[] offsetBuffer = new byte[8];
            await stream.ReadExactlyAsync(offsetBuffer, 0, 8);
            long offsetToCommit = BitConverter.ToInt64(offsetBuffer, 0);

            // 4. Save to Brain
            await offsetManager.CommitOffset(group, topic, offsetToCommit);
            
            Console.WriteLine($"Committed: Group '{group}' on '{topic}' at offset {offsetToCommit}");
            break;
        }
        case 4:
        {
            Console.WriteLine("FETCH GROUP Request");

            // 1. Read Group (Reuse logic from case 3)
            byte[] groupLenBuffer = new byte[2];
            await stream.ReadExactlyAsync(groupLenBuffer, 0, 2);
            short groupLen = BitConverter.ToInt16(groupLenBuffer, 0);

            byte[] groupBuffer = new byte[groupLen];
            await stream.ReadExactlyAsync(groupBuffer, 0, groupLen);
            string group = Encoding.UTF8.GetString(groupBuffer);

            // 2. Read Topic (Reuse logic from case 3)
            byte[] topicLenBuffer = new byte[2];
            await stream.ReadExactlyAsync(topicLenBuffer, 0, 2);
            short topicLen = BitConverter.ToInt16(topicLenBuffer, 0);

            byte[] topicBuffer = new byte[topicLen];
            await stream.ReadExactlyAsync(topicBuffer, 0, topicLen);
            string topic = Encoding.UTF8.GetString(topicBuffer);

            // 3. Ask the Brain
            long storedOffset = offsetManager.GetOffset(group, topic);

            // 4. Reply to Client
            Console.WriteLine($"Sending stored offset {storedOffset} for Group '{group}'");
            await stream.WriteAsync(BitConverter.GetBytes(storedOffset));
            
            break;
        }
        default:
            Console.WriteLine("Unknown Command");
            break;
    }
}
    }
}

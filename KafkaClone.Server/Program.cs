using System.Net;
using System.Net.Sockets;
using System.Text;
using KafkaClone.Storage;

string directory = Directory.GetCurrentDirectory();
string fullPath = Path.Combine(directory, "test.log");

Console.WriteLine($"Saving logs to: {fullPath}");

TopicManager topicManager = new TopicManager(Path.Combine(Directory.GetCurrentDirectory(), "kafka-data"));

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
    _ = HandleClientAsync(client, topicManager);
}



static async Task HandleClientAsync(TcpClient client,TopicManager topicManager)
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
            Console.WriteLine("PRODUCE Request");
            // 1. Get topic size
            byte[] topicSizeBuffer = new byte[2];
            await stream.ReadExactlyAsync(topicSizeBuffer,0,2);
            short topicLength = BitConverter.ToInt16(topicSizeBuffer);
            // 2. Read topic data and get Topic from TopicManager
            byte[] topicBuffer = new byte[topicLength];
            await stream.ReadExactlyAsync(topicBuffer,0,topicLength);
            string topicName = Encoding.UTF8.GetString(topicBuffer);

            LogSegment logSegment = topicManager.GetTopic(topicName);

            //Same as before
            byte[] sizeBuffer = new byte[4];
            await stream.ReadExactlyAsync(sizeBuffer,0,4);
            int size = BitConverter.ToInt32(sizeBuffer,0);
            byte[] payload = new byte[size];
            await stream.ReadExactlyAsync(payload,0,size);
            await logSegment.AppendAsync(payload);
            Console.WriteLine($"Saved {size} bytes to disk.");
            break;
        }
        case 2:
        {
            Console.WriteLine("CONSUME Request");
            byte[] topicSizeBuffer = new byte[2];
            await stream.ReadExactlyAsync(topicSizeBuffer,0,2);
            short topicLength = BitConverter.ToInt16(topicSizeBuffer);
            // 2. Read topic data and get Topic from TopicManager
            byte[] topicBuffer = new byte[topicLength];
            await stream.ReadExactlyAsync(topicBuffer,0,topicLength);
            string topicName = Encoding.UTF8.GetString(topicBuffer);

            LogSegment logSegment = topicManager.GetTopic(topicName);
            // New byte[] to store request size
            byte[] offsetBytes = new byte[8];
            await stream.ReadExactlyAsync(offsetBytes,0,8);
            long offset = BitConverter.ToInt64(offsetBytes,0);
            if (offset >= logSegment.Length)
            {
                // Send 8 bytes because the client expects a 'long'
                long endOfFileSignal = -1;
                await stream.WriteAsync(BitConverter.GetBytes(endOfFileSignal));
                break;
            }
            byte[] messageData = await logSegment.ReadAsync(offset);
            byte[] sizeBytes = BitConverter.GetBytes(messageData.Length);
            long nextOffset = offset + 4 + messageData.Length;
            await stream.WriteAsync(BitConverter.GetBytes(nextOffset));
            await stream.WriteAsync(sizeBytes);
            await stream.WriteAsync(messageData);
            break;
        }
        default:
            Console.WriteLine("Unknown Command");
            break;
    }
}
    }
}

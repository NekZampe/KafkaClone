using System.Text;
using System.Threading.Tasks;
using KafkaClone.Storage;
using System.Net;
using System.Net.Sockets;

// 1. Connect to the server
using TcpClient client = new TcpClient();
await client.ConnectAsync("127.0.0.1", 9092);
Console.WriteLine("Connected to server!");

using NetworkStream stream = client.GetStream();

// The Message
string message = "Hello Server";
byte[] payload = Encoding.UTF8.GetBytes(message);

Console.WriteLine("Connected! Type a message and press Enter:");

Console.WriteLine("Connected! Usage:");
Console.WriteLine("  Type '1 <topic> <message>' to PRODUCE (e.g., '1 Payments 344')");
Console.WriteLine("  Type '2 <topic> <offset>'  to CONSUME (e.g., '2 Payments 0')");
Console.WriteLine("  Type '3 <topic>         '  to CONSUME ALL (e.g., '3 Payments')");

while (true)
{
    Console.Write("> ");
    string input = Console.ReadLine();
    if (string.IsNullOrEmpty(input)) break;

    string[] parts = input.Split(' ', 3); // Split into "Command", "topic" and "Rest"
    string command = parts[0];
    string topic = parts[1];

    if (command == "1" && parts.Length > 1)
    {  
        string msg = parts[2];
        await ProduceAsync(stream,topic, msg);
    }
    else if (command == "2" && parts.Length > 1)
    {
        if (long.TryParse(parts[2], out long offset))
        {   
            long result = await ConsumeAsync(stream,topic,offset);
            Console.WriteLine($"Next Offset: {result}");
        }
        else
        {
            Console.WriteLine("Invalid offset.");
        }
    }else if (command == "3")
{
    Console.WriteLine("Streaming from beginning...");
    // Start from offset 0 and go until -1
    await ConsumeAllAsync(stream, topic, 0); 
}
    else
    {
        Console.WriteLine("Unknown command. Use '1 <msg>' or '2 <offset>'");
    }
}


// Method to write
static async Task ProduceAsync(NetworkStream stream,string topic, string message)
{
    
    // 1. Prepare the Data
    byte[] payload = Encoding.UTF8.GetBytes(message);
    byte[] sizeBytes = BitConverter.GetBytes(payload.Length);
    
    // 2. Send Command (1 = Produce)
    await stream.WriteAsync(new byte[] { 1 });

    //3.Send topic size
    byte[] topicBytes = Encoding.UTF8.GetBytes(topic);
    byte[] topicLengthBytes = BitConverter.GetBytes((short)topicBytes.Length);

    await stream.WriteAsync(topicLengthBytes);
    await stream.WriteAsync(topicBytes);

    // 3. Send Size (4 bytes)
    await stream.WriteAsync(sizeBytes);

    // 4. Send Payload
    await stream.WriteAsync(payload);
}

static async Task<long> ConsumeAsync(NetworkStream stream,string topic,long offset)
{
    // 1. Send Command (2 = Consume)
    await stream.WriteAsync(new byte[] { 2 });

    //1.5 
    byte[] topicBytes = Encoding.UTF8.GetBytes(topic);
    byte[] topicLengthBytes = BitConverter.GetBytes((short)topicBytes.Length);

    await stream.WriteAsync(topicLengthBytes);
    await stream.WriteAsync(topicBytes);

    // 2. Send Offset (8 bytes)
    byte[] offsetBytes = BitConverter.GetBytes(offset);
    await stream.WriteAsync(offsetBytes);

    // 2.1 Read nextOffset
    byte[] nextOffsetBuffer = new byte[8];

    try{
    await stream.ReadExactlyAsync(nextOffsetBuffer, 0, 8);

    }
    catch (EndOfStreamException)
    {
        Console.WriteLine("End of Log (Server disconnected).");
        return -1;
    }
    long nextOffset = BitConverter.ToInt64(nextOffsetBuffer,0);

    if (nextOffset == -1)
    {
        Console.WriteLine("End of Log.");
        return -1;
    }

    // 3. Read Response Size (4 bytes)
    byte[] sizeBuffer = new byte[4];
    await stream.ReadExactlyAsync(sizeBuffer, 0, 4);
    int size = BitConverter.ToInt32(sizeBuffer, 0);

    // 4. Read Response Payload
    byte[] payload = new byte[size];
    await stream.ReadExactlyAsync(payload, 0, size);

    // 5. Print it!
    string message = Encoding.UTF8.GetString(payload);
    Console.WriteLine($"Received from offset {offset}: {message}");
    return nextOffset;
}

static async Task ConsumeAllAsync(NetworkStream stream,string topic, long offset)
{

    while ( offset != -1)
    {
        offset = await ConsumeAsync(stream,topic,offset);
    }

    return;
    
}
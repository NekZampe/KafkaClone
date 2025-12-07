using System.Text;
using System.Threading.Tasks;
using KafkaClone.Storage;
using System.Net;
using System.Net.Sockets;
using System.Globalization;

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
Console.WriteLine("  Type '1 <topic> <message>' to PRODUCE (e.g., '1 payments 344')");
Console.WriteLine("  Type '2 <topic> <offset>'  to CONSUME (e.g., '2 payments 0')");
Console.WriteLine("  Type '3 <GroupName> <topic> <offset>'  to COMMIT (e.g., '3 groupA payments 67')"); //Commit groups offset
Console.WriteLine("  Type '4 <GroupName> <topic> '  to FETCH (e.g., '4 groupA payments')"); //Retrieves groups offset
Console.WriteLine("  Type '5 <TestNumber> <NumberOfMessages> '  to Run Load Test (e.g., '5 1 85')");

// MAIN LOOP
while (true)
{
    Console.Write("> ");
    string? input = Console.ReadLine();
    if (string.IsNullOrEmpty(input)) break;

    // 1. SPLIT ONCE: Max 4 parts (Command, Group, Topic, Offset/Message)
    // This covers your longest command (Case 3).
    string[] parts = input.Split(' ', 4);
    string command = parts[0];

    switch (command)
    {
        case "1": // Produce: 1 <topic> <message>
            if (parts.Length < 3)
            {
                Console.WriteLine("Usage: 1 <topic> <message>");
                break;
            }
            // parts[1] is topic, parts[2] is message
            await ProduceAsync(stream, parts[1], parts[2]);
            break;

        case "2": // Consume: 2 <topic> <offset>
            if (parts.Length < 3)
            {
                Console.WriteLine("Usage: 2 <topic> <offset>");
                break;
            }
            if (long.TryParse(parts[2], out long consumeOffset))
            {
                long result = await ConsumeAsync(stream, parts[1], consumeOffset);
                Console.WriteLine($"Next Offset: {result}");
            }
            else
            {
                Console.WriteLine("Invalid offset.");
            }
            break;

        case "3": // Commit: 3 <group> <topic> <offset>
            if (parts.Length < 4)
            {
                Console.WriteLine("Usage: 3 <group> <topic> <offset>");
                break;
            }
            if (long.TryParse(parts[3], out long commitOffset))
            {
                await CommitGroupOffsetAsync(stream, parts[1], parts[2], commitOffset);
                Console.WriteLine($"Committed to group: {parts[1]}");
            }
            else
            {
                Console.WriteLine("Invalid offset.");
            }
            break;

        case "4": // Fetch Group: 4 <group> <topic>
            if (parts.Length < 3)
            {
                Console.WriteLine("Usage: 4 <group> <topic>");
                break;
            }
            long fetchResult = await FetchGroupOffsetAsync(stream, parts[1], parts[2]);
            Console.WriteLine($"Current Offset: {fetchResult}");
            break;
        case "5":
            if (parts.Length < 3)
            {
                Console.WriteLine("Usage: 5 <int> <int>");
                break;
            }
            int.TryParse(parts[1],out int testNumber);
            int.TryParse(parts[2],out int amount);

            await RunLoadTest(stream,testNumber,amount);
            break;
        default:
            Console.WriteLine("Unknown command.");
            break;
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

static async Task CommitGroupOffsetAsync(NetworkStream stream,string group,string topic, long offset)
{
    // 0. Send Command (3 = Commit)
    await stream.WriteAsync(new byte[] { 3 });

    // 1. Convert group to byte[], get length 2 bytes
    byte[] groupBytes = Encoding.UTF8.GetBytes(group);
    byte[] lenGroupBytes = BitConverter.GetBytes((short)groupBytes.Length);
    // send it
    await stream.WriteAsync(lenGroupBytes);
    await stream.WriteAsync(groupBytes);

    //2. Convert group to bytes ( 2 bytes)
    byte[] topicBytes = Encoding.UTF8.GetBytes(topic);
    byte[] topicLengthBytes = BitConverter.GetBytes((short)topicBytes.Length);

    await stream.WriteAsync(topicLengthBytes);
    await stream.WriteAsync(topicBytes);

    // 2. Send Offset (8 bytes)
    byte[] offsetBytes = BitConverter.GetBytes(offset);
    await stream.WriteAsync(offsetBytes);
    
}

static async Task<long> FetchGroupOffsetAsync(NetworkStream stream,string group,string topic)
{
    // 0. Send Command (4 = Fetch)
    await stream.WriteAsync(new byte[] { 4 });

    // 1. Convert group to byte[], get length 2 bytes
    byte[] groupBytes = Encoding.UTF8.GetBytes(group);
    byte[] lenGroupBytes = BitConverter.GetBytes((short)groupBytes.Length);
    // send it
    await stream.WriteAsync(lenGroupBytes);
    await stream.WriteAsync(groupBytes);

    //2. Convert group to bytes ( 2 bytes)
    byte[] topicBytes = Encoding.UTF8.GetBytes(topic);
    byte[] topicLengthBytes = BitConverter.GetBytes((short)topicBytes.Length);

    await stream.WriteAsync(topicLengthBytes);
    await stream.WriteAsync(topicBytes);

    //3. Create buffer to read response ( offset ) and return it
    byte[] offsetBuffer = new byte[8];

    try{
    await stream.ReadExactlyAsync(offsetBuffer, 0, 8);

    }
    catch (EndOfStreamException)
    {
        Console.WriteLine($"End of Log for Group {group}");
        return -1;
    }

    return BitConverter.ToInt64(offsetBuffer,0);

}


static async Task RunLoadTest(NetworkStream stream,int testNumber,int amount)
{
    string topic = "test-" + testNumber.ToString();

    for(int i= 0; i < amount; i++){

        string message = "message-" + i.ToString();

        await ProduceAsync(stream,topic,message);
        
    }

    // int result = await ConsumeAllAsync(stream,topic);

    Console.WriteLine("----------TEST COMPLETE----------");
    Console.WriteLine($"Total Messages Sent: {amount}");
    // Console.WriteLine($"Total Messages Read: {result-1}");

}


// static async Task<int> ConsumeAllAsync(NetworkStream stream,string topic)
// {
//     long offset = 0;
//     int total = 0;

//     while(offset != -1)
//     {
//         offset = await ConsumeAsync(stream,topic,offset);
//         total++;
//     }

//     return total;

// }
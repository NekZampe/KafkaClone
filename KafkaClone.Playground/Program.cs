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
Console.WriteLine("  Type '0 <topic> <partitionCount>' to CREATE NEW TOPIC (e.g., '0 payments 3')");
Console.WriteLine("  Type '1 <topic> <message>' to PRODUCE (e.g., '1 payments 344')");
Console.WriteLine("  Type '2 <topic> <partitionId> <offset>'  to CONSUME (e.g., '2 payments 0 -200$')");
Console.WriteLine("  Type '3 <group> <topic> <partitionId> <offset>'  to COMMIT (e.g., '3 groupA payments 0 34')"); //Commit groups offset
Console.WriteLine("  Type '4 <group> <topic> <partitionId>'  to FETCH (e.g., '4 groupA payments 0')"); //Retrieves groups offset
Console.WriteLine("  Type '5 <topic> <message1>,<message2>...' to BATCH PRODUCE (e.g., '5 payments +344$ -22$ +3758$')");
Console.WriteLine("  Type '6 <topic> <offset> <NumberOfMessages>' to BATCH CONSUME (e.g., '6 payments 5 100')"); // Starting at offset 5, read the next 100 messages
Console.WriteLine("  Type 'test <topic> <NumberOfMessages>' to Run Load Test (e.g., 'test payments 100')");

// MAIN LOOP
while (true)
{
    Console.Write("> ");
    string? input = Console.ReadLine();
    if (string.IsNullOrEmpty(input)) break;

    // 1. SPLIT ONCE: Max 4 parts (Command, Group, Topic, key, Offset/Message)
    // This covers your longest command (Case 3).
    string[] parts = input.Split(' ', 5);
    string command = parts[0];

    switch (command)
    {
        case "0": // Create: 0 <topic> <partitionCount>
            {
                if (parts.Length < 3)
                {
                    Console.WriteLine("Usage: 0 <topic> <partitionCount>");
                    break;
                }

                if (int.TryParse(parts[2], out int count))
                {
                    await CreateTopicAsync(stream, parts[1], count);
                    Console.WriteLine($"Request sent to create topic '{parts[1]}' with {count} partitions.");
                }
                break;
            }
        case "1": // Produce: 1 <topic> <message>
            if (parts.Length < 3)
            {
                Console.WriteLine("Usage: 1 <topic> <message>");
                break;
            }
            // parts[1] is topic, parts[2] is message
            await ProduceAsync(stream, parts[1], parts[2]);
            break;

        case "2": // Consume: 2 <topic> <partitionId> <offset>
            {
                if (parts.Length < 4)
                {
                    Console.WriteLine("Usage: 2 <topic> <partitionId> <offset>");
                    break;
                }
                if (int.TryParse(parts[2], out int partitionId) && long.TryParse(parts[3], out long consumeOffset))
                {
                    if (partitionId < 0) partitionId = -1;
                    long result = await ConsumeAsync(stream, parts[1], partitionId, consumeOffset);
                    Console.WriteLine($"Next Offset: {result}");
                }
                else
                {
                    Console.WriteLine("Invalid offset.");
                }
                break;
            }
        case "3": // Commit: 3 <group> <topic> <partitionId> <offset>
            {
                if (parts.Length < 5)
                {
                    Console.WriteLine("Usage: 3 <group> <topic> <partitionId> <offset>");
                    break;
                }
                if (int.TryParse(parts[3], out int partitionId) && long.TryParse(parts[4], out long commitOffset))
                {
                    await CommitGroupOffsetAsync(stream, parts[1], parts[2], partitionId, commitOffset);
                    Console.WriteLine($"Committed: Group '{parts[1]}' on '{parts[2]}' part {partitionId} at offset {commitOffset}");
                }
                else
                {
                    Console.WriteLine("Invalid offset.");
                }
                break;
            }
        case "4": // Fetch Group: 4 <group> <topic> <partitionId>
            {
                if (parts.Length < 4)
                {
                    Console.WriteLine("Usage: 4 <group> <topic> <partitionId>");
                    break;
                }
                if (int.TryParse(parts[3], out int partitionId))
                {

                    long fetchResult = await FetchGroupOffsetAsync(stream, parts[1], parts[2], partitionId);
                    Console.WriteLine($"Current Offset: {fetchResult}");
                }
                else
                {
                    Console.WriteLine("Invalid partition ID.");
                }
                break;
            }

        case "5": // batch Produce: 1 <topic> <messageCount> <message1> <message2>...
            {
                string[] batchParts = input.Split(' ', 3);
                if (parts.Length < 3)
                {
                    Console.WriteLine("Usage: 5 <topic> <messageCount> <message1> <message2>...");
                    break;
                }
                await ProduceBatchAsync(stream, batchParts[1], batchParts[2]);

                break;
            }
        case "test":
            if (parts.Length < 3)
            {
                Console.WriteLine("Usage: 5 <topic> <NumberOfMessages>");
                break;
            }

            if (int.TryParse(parts[2], out int amount))
            {

                await RunLoadTest(stream, parts[1], amount);
            }
            break;
        default:
            Console.WriteLine("Unknown command.");
            break;
    }
}

static async Task CreateTopicAsync(NetworkStream stream, string topic, int count)
{
    // 0. Send Command (0)
    await stream.WriteAsync(new byte[] { 0 });

    // 1. Send Topic Name
    byte[] topicBytes = Encoding.UTF8.GetBytes(topic);
    byte[] topicLengthBytes = BitConverter.GetBytes((short)topicBytes.Length);
    await stream.WriteAsync(topicLengthBytes);
    await stream.WriteAsync(topicBytes);

    // 2. Send Partition Count
    byte[] countBytes = BitConverter.GetBytes(count);
    await stream.WriteAsync(countBytes);

    // 3. Wait for Ack (1 byte)
    byte[] ack = new byte[1];
    await stream.ReadExactlyAsync(ack, 0, 1);
}

// Produce single message
static async Task ProduceAsync(NetworkStream stream, string topic, string message)
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

static async Task<long> ConsumeAsync(NetworkStream stream, string topic, int partitionId, long offset)
{
    // 0. Send Command (2 = Consume)
    await stream.WriteAsync(new byte[] { 2 });

    // 1. Send Topic
    byte[] topicBytes = Encoding.UTF8.GetBytes(topic);
    byte[] topicLengthBytes = BitConverter.GetBytes((short)topicBytes.Length);

    await stream.WriteAsync(topicLengthBytes);
    await stream.WriteAsync(topicBytes);

    // 2. Send Partition ID (4 Bytes -> int)
    byte[] partIdBytes = BitConverter.GetBytes(partitionId);
    await stream.WriteAsync(partIdBytes);

    // 3. Send Offset (8 bytes)
    byte[] offsetBytes = BitConverter.GetBytes(offset);
    await stream.WriteAsync(offsetBytes);

    // 2.1 Read nextOffset
    byte[] nextOffsetBuffer = new byte[8];

    try
    {
        await stream.ReadExactlyAsync(nextOffsetBuffer, 0, 8);

    }
    catch (EndOfStreamException)
    {
        Console.WriteLine("End of Log (Server disconnected).");
        return -1;
    }
    long nextOffset = BitConverter.ToInt64(nextOffsetBuffer, 0);

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

static async Task CommitGroupOffsetAsync(NetworkStream stream, string group, string topic, int partitionId, long offset)
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

    // 2. Send Partition ID (4 Bytes -> int)
    byte[] partIdBytes = BitConverter.GetBytes(partitionId);
    await stream.WriteAsync(partIdBytes);

    // 2. Send Offset (8 bytes)
    byte[] offsetBytes = BitConverter.GetBytes(offset);
    await stream.WriteAsync(offsetBytes);

}

static async Task<long> FetchGroupOffsetAsync(NetworkStream stream, string group, string topic, int partitionId)
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

    // 3. Send Partition ID (4 Bytes -> int)
    byte[] partIdBytes = BitConverter.GetBytes(partitionId);
    await stream.WriteAsync(partIdBytes);

    //3. Create buffer to read response ( offset ) and return it
    byte[] offsetBuffer = new byte[8];

    try
    {
        await stream.ReadExactlyAsync(offsetBuffer, 0, 8);

    }
    catch (EndOfStreamException)
    {
        Console.WriteLine($"End of Log for Group {group}");
        return -1;
    }

    return BitConverter.ToInt64(offsetBuffer, 0);

}


static async Task RunLoadTest(NetworkStream stream, string topic, int amount)
{

    for (int i = 0; i < amount; i++)
    {
        string message = "message-" + i.ToString();

        await ProduceAsync(stream, topic, message);
    }

    Console.WriteLine("----------TEST COMPLETE----------");
    Console.WriteLine($"Total Messages Sent: {amount}");

}


// Produce Batch Method
static async Task ProduceBatchAsync(NetworkStream stream, string topic, string messages)
{
    // 1. Send cmd
    await stream.WriteAsync(new byte[] { 5 });

    // 2. Send topic
    byte[] topicBytes = Encoding.UTF8.GetBytes(topic);
    byte[] topicLengthBytes = BitConverter.GetBytes((short)topicBytes.Length);

    await stream.WriteAsync(topicLengthBytes);
    await stream.WriteAsync(topicBytes);

    var options = StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries;
    string[] parts = messages.Split(',', options);

    int msgCount = parts.Length;

    // 3. Send message count
    byte[] msgCountBytes = BitConverter.GetBytes(msgCount);
    await stream.WriteAsync(msgCountBytes);

    List<byte> payload = new List<byte>();

    foreach (var part in parts)
    {
        byte[] msg = Encoding.UTF8.GetBytes(part);
        byte[] msgLen = BitConverter.GetBytes(msg.Length);

        payload.AddRange(msgLen);
        payload.AddRange(msg);
    }

    await stream.WriteAsync(payload.ToArray());
}


static async Task<long> ConsumeBatchAsync(NetworkStream stream, string topic, int partitionId, long offset,int maxCount)
{
    // 0. Send Command (6 = Consume batch)
    await stream.WriteAsync(new byte[] { 6 });

    // 1. Send Topic
    byte[] topicBytes = Encoding.UTF8.GetBytes(topic);
    byte[] topicLengthBytes = BitConverter.GetBytes((short)topicBytes.Length);

    await stream.WriteAsync(topicLengthBytes);
    await stream.WriteAsync(topicBytes);

    // 2. Send Partition ID (4 Bytes -> int)
    byte[] partIdBytes = BitConverter.GetBytes(partitionId);
    await stream.WriteAsync(partIdBytes);

    // 3. Send Offset (8 bytes)
    byte[] offsetBytes = BitConverter.GetBytes(offset);
    await stream.WriteAsync(offsetBytes);

    // 2.1 Read nextOffset
    byte[] nextOffsetBuffer = new byte[8];

    try
    {
        await stream.ReadExactlyAsync(nextOffsetBuffer, 0, 8);

    }
    catch (EndOfStreamException)
    {
        Console.WriteLine("End of Log (Server disconnected).");
        return -1;
    }
    long nextOffset = BitConverter.ToInt64(nextOffsetBuffer, 0);

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
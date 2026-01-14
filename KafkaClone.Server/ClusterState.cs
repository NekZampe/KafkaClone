using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaClone.Storage;
using KafkaClone.Shared;
using KafkaClone.Server.DTOs;
using System.Text.Json;

namespace KafkaClone.Server;

public class ClusterState
{
    // === SINGLE SOURCE OF TRUTH ===
    // 1. Map of Topics
    private Dictionary<string, TopicMetaData> _topicMap = new();
    
    // 2. Map of Brokers
    private Dictionary<int, Broker> _brokers = new();
    private readonly object _lock = new object();
    private readonly string _filePath;

    private ClusterState(string basePath,List<Broker> brokers)
    {
        _filePath = Path.Combine(basePath, "cluster_state.json");

        foreach(var broker in brokers)
        {
            _brokers[broker.Id] = broker;
        }

    }

    protected ClusterState() 
    { 
        // Moq needs this to create the proxy object.
    }

    public virtual Dictionary<int, int> CalculateAssignments(int partitions)
    {
        var assignments = new Dictionary<int, int>();
        var activeBrokers = _brokers.Keys.OrderBy(x => x).ToList();

        if (activeBrokers.Count == 0) throw new Exception("No active brokers!");

        for (int i = 0; i < partitions; i++)
        {
            int assignedBrokerId = activeBrokers[i % activeBrokers.Count];
            assignments.Add(i, assignedBrokerId);
        }
        return assignments;
    }

    public static async Task<ClusterState> InitializeAsync(string basePath,List<Broker> brokers)
    {
        var state = new ClusterState(basePath,brokers);
        await state.LoadFromDisk();
        return state;
    }

    private async Task LoadFromDisk()
    {
        if (!File.Exists(_filePath)) return; // First run, nothing to load

        try 
        {
            string json = await File.ReadAllTextAsync(_filePath);
            
            // We use a DTO for cleaner serialization
            var snapshot = JsonSerializer.Deserialize<ClusterSnapshotDTO>(json);
            
            if (snapshot != null)
            {
                _topicMap = snapshot.Topics ?? new();
                _brokers = snapshot.Brokers ?? new();
                Console.WriteLine($"[ClusterState] Loaded {_topicMap.Count} topics and {_brokers.Count} brokers from disk.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ClusterState] Error loading state: {ex.Message}");
        }
    }

    // === WRITE PATH (Called by RaftNode) ===
    public virtual bool ApplyCommand(IClusterCommand command)
    {
        lock (_lock)
        {
            switch (command)
            {
                case RegisterBroker cmd:
                    // Update our list of alive brokers
                    if (!_brokers.ContainsKey(cmd.Id))
                    {
                        _brokers[cmd.Id] = new Broker(cmd.Id, cmd.Port, cmd.GrpcPort, cmd.Host);
                        Console.WriteLine($"[ClusterState] Registered Broker {cmd.Id}");
                        return true;
                    }
                    return false;

                case CreateTopic cmd:
                    if (_topicMap.ContainsKey(cmd.Name)) return false;

                    var meta = new TopicMetaData
                    {
                        TopicName = cmd.Name
                    };

                    // === THE ASSIGNMENT LOGIC ===
                    // We use the local _brokers list to decide assignments
                    var availableBrokers = _brokers.Keys.OrderBy(x => x).ToList();
                    
                    if (availableBrokers.Count == 0)
                    {
                        Console.WriteLine($"[Error] Cannot create topic '{cmd.Name}'. No brokers registered!");
                        return false;
                    }

                    for (int i = 0; i < cmd.Partitions; i++)
                    {
                        // Round Robin Math
                        int assignedBrokerId = availableBrokers[i % availableBrokers.Count];

                        meta.PartitionMetadata.Add(new PartitionMetaData
                        {
                            PartitionId = i,
                            BrokerId = assignedBrokerId
                        });
    
                    }
                    
                    _topicMap[cmd.Name] = meta;
                    Console.WriteLine($"[ClusterState] Created Topic '{cmd.Name}' with {cmd.Partitions} partitions.");
                    return true;

                    default:
                    return false;
            }
        }
    }

    // === READ PATH (Used by BrokerService & TCP) ===

    public virtual int? GetPartitionOwner(string topic, int partitionId)
    {
        lock (_lock)
        {
            if (_topicMap.TryGetValue(topic, out var meta))
            {
                var part = meta.PartitionMetadata.FirstOrDefault(p => p.PartitionId == partitionId);
                return part?.BrokerId;
            }
            return null;
        }
    }

    public List<Broker> GetAllBrokers()
    {
        lock (_lock)
        {
            return _brokers.Values.ToList();
        }
    }
    
// Returns: (Byte Array of all brokers, Count of brokers)
public virtual async Task<(byte[], int)> GetSerializedBrokersAsync()
{
    List<Broker> snapshot;
    lock (_lock)
    {
        snapshot = _brokers.Values.ToList();
    }

    using var ms = new MemoryStream();
    foreach (var broker in snapshot)
    {
        // Serializes: [ID (4)][HostLength (4)][HostBytes (N)][Port (4)]
        byte[] currentBroker = broker.Serialize(); 
        await ms.WriteAsync(currentBroker);
    }

    return (ms.ToArray(), snapshot.Count);
}

// Returns: (Byte Array of partition assignments, Count of partitions)
public virtual async Task<(byte[], int)> GetSerializedTopicMetadataAsync(string topicName)
{
    TopicMetaData topic;
    
    // 1. Thread-safe lookup
    lock (_lock)
    {
        if (!_topicMap.TryGetValue(topicName, out topic))
        {
            // Topic doesn't exist: Return empty/zero
            return (Array.Empty<byte>(), 0);
        }
    }

    // 2. Serialize the Partition Assignments
    using var ms = new MemoryStream();
    
    // Iterate through the assignments (List<PartitionMetaData>)
    // Structure: [PartitionID (4 bytes)][BrokerID (4 bytes)]
    foreach (var partition in topic.PartitionMetadata)
    {
        // Write Partition ID
        await ms.WriteAsync(BitConverter.GetBytes(partition.PartitionId));
        
        // Write Broker ID (The Leader for this partition)
        await ms.WriteAsync(BitConverter.GetBytes(partition.BrokerId));
    }

    return (ms.ToArray(), topic.PartitionMetadata.Count);
}

    // === HELPER DTO FOR JSON ===
    public class ClusterSnapshotDTO
    {
        public Dictionary<string, TopicMetaData> Topics { get; set; }
        public Dictionary<int, Broker> Brokers { get; set; }
    }

    // Return all relevant partitionIds for a given topic based on broker Id
    public virtual async Task<int[]> GetListofTopicPartitionsByBrokerId(string topic, int brokerId)
    {
        List<int> partitionIds = new List<int>();

        lock (_lock)
        {
            if (_topicMap.TryGetValue(topic, out var data))
            {
                foreach (var partition in data.PartitionMetadata)
                {
                    if (partition.BrokerId == brokerId)
                    {
                        partitionIds.Add(partition.PartitionId);
                    }
                }
            }
        }

        return partitionIds.ToArray();
    }

}
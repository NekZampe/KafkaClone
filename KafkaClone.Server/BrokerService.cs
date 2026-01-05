using KafkaClone.Server;
using KafkaClone.Storage;
using KafkaClone.Shared;
using Microsoft.Extensions.Logging;
using KafkaClone.Server.DTOs;

namespace KafkaClone.Service;

public class BrokerService
{
    private readonly RaftNode _raftNode;
    private readonly TopicManager _topicManager;
    private readonly OffsetManager _offsetManager;
    private readonly Broker _myIdentity;
    private readonly ILogger<BrokerService> _logger;

    // This service is a Singleton that lives for the life of the application
    public BrokerService(
        RaftNode raftNode, 
        TopicManager topicManager, 
        OffsetManager offsetManager,
        Broker identity,
        ILogger<BrokerService> logger)
    {
        _raftNode = raftNode;
        _topicManager = topicManager;
        _offsetManager = offsetManager;
        _myIdentity = identity;
        _logger = logger;
    }

    // =========================================================
    //  Control Plane (Admin Operations -> Goes to Raft)
    // =========================================================

    // Create Topic
    // Fetch Cluster MetaData ( who has what )
    // Commit Consumer Group Offset
    // Fetch Consumer Group Offset

    public async Task<bool> CreateTopicAsync(string topicName, int partitions)
    {
        // 1. Validate input
        if (partitions <= 0) return false;

        // 2. Create the command
        var command = new CreateTopic
        {
            Name = topicName,
            Partitions = partitions
        };

        // 3. Propose to Raft (The ClusterState will handle the logic)
        var result = await _raftNode.Propose(command);

        return result.Success;
    }

    public async Task<> FetchClusterMetaData










    // =========================================================
    //  Data Plane (Straight to Storage)
    // =========================================================

    public async Task ProducAsync(string topicName,int partitionId, byte[] payload)
    {
        // 1. Get the physical partition
        var partition = _topicManager.GetTopicPartitionById(topicName, partitionId);
        // 2. Append to the log file
        await partition.AppendAsync(payload);

    }

    public async Task BatchProduceAsync(string topicName,int partitionId,List<byte[]> payloads)
    {
        var partition = _topicManager.GetTopicPartitionById(topicName, partitionId);
        await partition.AppendBatchAsync(payloads);
    }

    public async Task<byte[]> ConsumeAsync(string topicName,int partitionId,long offset)
    {
        var partition = _topicManager.GetTopicPartitionById(topicName, partitionId);

        byte[] messageData = await partition.ReadAsync(offset);

        return messageData;
    }

    public async Task<(List<byte[]> messages, long nextOffset)> BatchConsumeAsync(string topicName,int partitionId, long offset, int count)
    {
        var partition = _topicManager.GetTopicPartitionById(topicName, partitionId);

        var response = await partition.ReadBatchAsync(offset, count);

        return response;
        
        
    }
}
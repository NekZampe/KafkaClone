using KafkaClone.Server;
using KafkaClone.Storage;
using KafkaClone.Shared;
using Microsoft.Extensions.Logging;
using KafkaClone.Server.DTOs;

namespace KafkaClone.Service;

public class BrokerService
{
    private readonly Broker _myIdentity;  // Services Broker Identity
    private readonly RaftNode _raftNode; // Raftnode for Consensus
    private readonly TopicManager _topicManager; // Manages Topics
    private readonly OffsetManager _offsetManager; // Manages Offsets
    private readonly ILogger<BrokerService> _logger;

    private Object _lock;

    private Task? _applyLoopTask;
    private CancellationTokenSource _cts;

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

     public void Start()
    {
        _cts = new CancellationTokenSource();
        _applyLoopTask = Task.Run(() => ApplyLoop(_cts.Token));
    }
    
    public async Task Stop()
    {
        _cts?.Cancel();
        if (_applyLoopTask != null)
        {
            await _applyLoopTask;
        }
    }
    
    // Background loop that applies committed entries
    private async Task ApplyLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                // Check if there are committed entries to apply
                long commitIndex = _raftNode.LeaderCommit;
                long lastApplied = _raftNode.LastApplied;
                
                if (commitIndex > lastApplied)
                {
                    List<LogEntry> entries = await _raftNode.GetLogEntries(lastApplied,commitIndex);
                    // Apply all committed but not yet applied entries
                    foreach(var entry in entries)
                    {
                        var cmd = entry.Command;
                        await ApplyLogEntry(entry);
                        await _raftNode.UpdateLastApplied(entry.Index);
                    }
                }
                
                // Sleep briefly before checking again
                await Task.Delay(10, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in apply loop");
                await Task.Delay(100, ct); // Back off on error
            }
        }
    }
    
    // Apply a single log entry to the state machine
    private async Task ApplyLogEntry(LogEntry entry)
    {
        var command = entry.Command;

        await HandleCommand(command);
    }
    
     private async Task HandleCommand(IClusterCommand command)
    {
        lock (_lock) { /* Update _topics/_brokers dictionaries */ }

        // Trigger Physical Side Effects
        switch (command)
        {
            case CreateTopic cmd:
                 
                 await CreateTopicAsync(cmd);
                 break;
                 
        } 
    }

    // =========================================================
    //  Control Plane (Admin Operations -> Goes to Raft)
    // =========================================================

    public async Task<ForwardCommandResponse> CreateTopicAsync(CreateTopic cmd)
    {
        // 1. Validate input
        if (cmd.Partitions <= 0) return new ForwardCommandResponse
        {
            Success = false,
            ErrorMessage = $"Not enough Partitions"
        };

        // 3. Propose to Raft (The ClusterState will handle the logic)
        var result = await _raftNode.Propose(cmd);

         if ( result.Success){   
            return new ForwardCommandResponse
            {
                Success = true,
                ErrorMessage = null
            };
         }
        else return new ForwardCommandResponse
        {
            Success = false,
            ErrorMessage = $"Error creating Topic: {cmd.Name}"
        };
    }

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

    public async Task<long> FetchOffset(string group,string topic, int partitionId)
    {
        return _offsetManager.GetOffset(group,topic,partitionId);
    }

    public async Task CommitGroupOffset(string group,string topic,int partitionId,long offset)
    {
        await _offsetManager.CommitOffset(group, topic, partitionId, offset);
    }


    public async Task<(byte[] data, int count)> GetTopicMetadata(string topic)
    {
        return await _raftNode.GetSerializedTopicMetadataAsync(topic);
    }

    public async Task<(byte[] data, int count)> GetBrokerMetadata()
    {
        return await _raftNode.GetSerializedBrokersAsync();
    }



}
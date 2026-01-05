using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaClone.Storage;
using KafkaClone.Shared;
using KafkaClone.Server.DTOs;

namespace KafkaClone.Server;

public class ClusterState
{
    // In-Memory Metadata
    private readonly Dictionary<string, TopicData> _topics = new();
    private readonly Dictionary<int, Broker> _brokers = new();
    
    // Physical Storage Reference
    private readonly TopicManager _topicManager; 
    private readonly OffsetManager _offsetManager;

    private Object _lock;

    public ClusterState(string basePath, TopicManager topicManager, OffsetManager offsetManager)
    {
        _topicManager = topicManager;
        _offsetManager = offsetManager;
    }

    public async Task ApplyCommand(IClusterCommand command)
    {
        lock (_lock) { /* Update _topics/_brokers dictionaries */ }

        // Trigger Physical Side Effects
        switch (command)
        {
            case CreateTopic cmd:
                 // Create physical files on disk
                 await _topicManager.CreateTopic(cmd.Name, cmd.Partitions, ...); 
                 break;
                 
            case ConsumerOffset cmd:
                 // Update offset file
                 await _offsetManager.CommitOffset(...);
                 break;
        }
    }
}
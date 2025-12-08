
using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace KafkaClone.Storage;

public class TopicManager
{

    private readonly string _basePath;

    // Topic name (unique), Partition obj
    private Dictionary<string,List<Partition>> _topics;

    private readonly ILoggerFactory _loggerFactory;

    private Dictionary<string, int> _roundRobinCounters;

    private int _defaultPartitions;

    public TopicManager(string basePath, ILoggerFactory loggerFactory, int defaultPartitions = 1)
    {
        _basePath = basePath;
        _loggerFactory = loggerFactory;
        _topics = new Dictionary<string,List<Partition>>();
        _roundRobinCounters = new Dictionary<string, int>();
        _defaultPartitions = defaultPartitions;
    }


    public Partition GetTopic(string topic,int index = -1)
    {
        // Ensure valid param
        if (string.IsNullOrEmpty(topic)) return null;

        // If topic exists return logSegment right away based on provided index if any
        if (_topics.TryGetValue(topic, out var partitions))
        {
            if (index < 0 )
                return RoundRobin(topic, partitions);

            if (index <= partitions.Count() - 1)
                return partitions[index];
        }
        
        // Partition doesn't exist so create it
        // First verify base path exists
        if (!Directory.Exists(_basePath))
        {
            Directory.CreateDirectory(_basePath);
        }

        ILogger<Partition> partitionLogger = _loggerFactory.CreateLogger<Partition>();

        List<Partition> newPartitions = new List<Partition>();
        
    
        for(int i = 0; i < _defaultPartitions; i++)
        {
            string newFolder = Path.Combine(_basePath,topic,$"{topic}-{i}");
            Partition newPartition = new Partition(newFolder,true, partitionLogger,TimeSpan.FromSeconds(20));
            newPartitions.Add(newPartition);
        }

        _topics[topic] = newPartitions;
        _roundRobinCounters[topic] = 0;

        if (index <= newPartitions.Count() - 1 && index > -1)
            return newPartitions[index];

        return RoundRobin(topic,newPartitions);
    }

    private Partition RoundRobin(string topic, List<Partition> partitions)
    {

        if (partitions.Count() == 1) return partitions[0];
        
        _roundRobinCounters.TryGetValue(topic, out var current);

        int next = (current + 1) %  partitions.Count();

        _roundRobinCounters[topic] = next;

        return partitions[next];
    }
}
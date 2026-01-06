
using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using KafkaClone.Shared;
using System.Threading.Tasks;

namespace KafkaClone.Storage;

public class TopicManager
{

    private readonly string _basePath;

    // Topic name (unique), Partition obj
    private Dictionary<string, List<Partition>> _topics;

    private readonly ILoggerFactory _loggerFactory;

    private Dictionary<string,TopicState> _topicStates;

    private int _defaultPartitions = 3;

    private TopicManager(string basePath, ILoggerFactory loggerFactory, int defaultPartitions)
    {
        _basePath = basePath;
        _loggerFactory = loggerFactory;
        _topics = new Dictionary<string, List<Partition>>();
        _topicStates = new Dictionary<string, TopicState>();
        _defaultPartitions = defaultPartitions;

    }


    public static async Task<TopicManager> InitializeAsync(string basePath, ILoggerFactory loggerFactory, int defaultPartitions = 1)
    {

        TopicManager topicManager = new TopicManager(basePath,loggerFactory,defaultPartitions);

        await topicManager.LoadTopics();

        return topicManager;
        
    }


    public async Task CreateTopic(string topic, int[] partitionIds,bool autoFlush,int maxFileSize, TimeSpan timeSpan)
    {
        // 1. Safety Check: Don't overwrite existing topics
        if (_topics.ContainsKey(topic) || Directory.Exists(Path.Combine(_basePath, topic)))
        {
            Console.WriteLine($"Topic '{topic}' already exists. Skipping creation.");
            return;
        }

        // 2. Prepare for creation
        if (!Directory.Exists(_basePath)) Directory.CreateDirectory(_basePath);

        ILogger<Partition> partitionLogger = _loggerFactory.CreateLogger<Partition>();
        List<Partition> newPartitions = new List<Partition>();

        int count = partitionIds.Length;

        TopicData tpd = new TopicData
            {
                TopicName = topic,
                AutoFlush = autoFlush,
                MaxFileSize = maxFileSize,
                TimeSpan = timeSpan
            };

        // 3. Create the specific number of partitions requested, save the state
        for (int i = 0; i < count; i++)
        {
            string newFolder = Path.Combine(_basePath, topic, $"{topic}-{partitionIds[i]}");

            Partition newPartition = new Partition(i,newFolder,autoFlush, partitionLogger, timeSpan, maxFileSize);
            newPartitions.Add(newPartition);


            tpd.PartitionIds.Add(partitionIds[i]);

        }

        string newPath = Path.Combine(_basePath,topic);

        TopicState ts = await TopicState.CreateAsync(newPath,tpd);

        // 4. Register it in memory
        _topicStates[topic] = ts;
        _topics[topic] = newPartitions;

        Console.WriteLine($"Created topic '{topic}' with {count} partitions.");
    }

    public Partition GetTopicPartitionById(string topic, int partitionId)
    {
        // Ensure valid param
        if (string.IsNullOrEmpty(topic)) return null;

        // If topic exists return logSegment right away based on provided index if any
        if (_topics.TryGetValue(topic, out var partitions))
        {
                return partitions[partitionId];
        }
        return null;
    }

    private async Task LoadTopics()
    {

        if (!Directory.Exists(_basePath)) Directory.CreateDirectory(_basePath);

        string[] drs = Directory.GetDirectories(_basePath);
        List<string> paths = new List<string>(drs.Length);

        foreach(var dr in drs)
        {
            paths.Add(Path.GetFileName(dr));
        }

        foreach( var topic in paths )
        {
            
            string topicPath = Path.Combine(_basePath,topic);
            string statePath = Path.Combine(topicPath,"state.json");

            if(!File.Exists(statePath))
                throw new FileNotFoundException($"Cannot find `state.json` file for {topic}");

            TopicState ts = await TopicState.LoadAsync(topicPath);

            if (ts is null) continue;

            List<Partition> newPartitions = new List<Partition>();

            _topicStates[topic] = ts;

            foreach(var id in ts.TopicData.PartitionIds ){

            ILogger<Partition> partitionLogger = _loggerFactory.CreateLogger<Partition>();

            int partitionId = id;
            bool autoFlush = ts.TopicData.AutoFlush;
            TimeSpan timeSpan = ts.TopicData.TimeSpan;
            int maxFileSize = ts.TopicData.MaxFileSize;


            string partitionPath = Path.Combine(topicPath,$"{topic}-{partitionId}");

            Partition newPartition = new Partition(partitionId,partitionPath,autoFlush, partitionLogger, timeSpan, maxFileSize);
            newPartitions.Add(newPartition);

            }

            _topics[topic] = newPartitions;

            }


        }


        
    }




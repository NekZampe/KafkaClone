
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
    private Dictionary<string,Partition> _topics;

    private readonly ILoggerFactory _loggerFactory;

    public TopicManager(string basePath, ILoggerFactory loggerFactory)
    {
        _basePath = basePath;
        _loggerFactory = loggerFactory;
        _topics = new Dictionary<string,Partition>();
    }


    public Partition GetTopic(string topic)
    {
        // Ensure valid param
        if (string.IsNullOrEmpty(topic)) return null;

        // If topic exists return logSegment right away
        if (_topics.TryGetValue(topic,out Partition value))
        {
            return value;
        }

        // Partition doesn't exist so create it
        // First verify base path exists
        if (!Directory.Exists(_basePath))
        {
            Directory.CreateDirectory(_basePath);
        }

        string newFolder = Path.Combine(_basePath,topic);

        ILogger<Partition> partitionLogger = _loggerFactory.CreateLogger<Partition>();
        
        Partition newLogSegment = new Partition(newFolder,false, partitionLogger,TimeSpan.FromSeconds(20));

        _topics.Add(topic,newLogSegment);

        return newLogSegment;

    }

    
}
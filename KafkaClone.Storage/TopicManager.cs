
using System;
using System.IO;
using System.Text;
using System.Collections.Generic;


namespace KafkaClone.Storage;

public class TopicManager
{

    private readonly string _basePath;

    // Topic name (unique), LogSegment obj
    private Dictionary<string,LogSegment> _topics;

    public TopicManager(string basePath)
    {
        _basePath = basePath;
        _topics = new Dictionary<string,LogSegment>();
    }


    public LogSegment GetTopic(string topic)
    {
        // Ensure valid param
        if (string.IsNullOrEmpty(topic)) return null;

        // If topic exists return logSegment right away
        if (_topics.TryGetValue(topic,out LogSegment value))
        {
            return value;
        }

        // LogSegment doesn't exist so create it
        // First verify base path exists
        if (!Directory.Exists(_basePath))
        {
            Directory.CreateDirectory(_basePath);
        }

        string newFolder = Path.Combine(_basePath,topic);

        Directory.CreateDirectory(newFolder);

        string newPath = Path.Combine(newFolder,"00000.log");
        
        LogSegment newLogSegment = new LogSegment(newPath,true);

        _topics.Add(topic,newLogSegment);

        return newLogSegment;

    }

    
}
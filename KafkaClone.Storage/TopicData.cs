using System;
using System.IO;
using System.Text;

namespace KafkaClone.Storage;

public class TopicData
{
    
    public required string TopicName {get;set;}
    public bool AutoFlush {get;set;}
    public int MaxFileSize {get;set;}
    public TimeSpan TimeSpan {get;set;}
    public List<int> PartitionIds {get;set;} =  new();

}
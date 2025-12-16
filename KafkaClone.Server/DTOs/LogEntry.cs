using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaClone.Server.DTOs
{

//-------------------------------- LOG TYPES ------------------------------------------------

    // 1. The Interface
    public interface IClusterCommand
    {
        string CommandType { get; }
    }

    // 2. Create Topic
    public class CreateTopic : IClusterCommand
    {
        public string CommandType => "CreateTopic";
        public string Name { get; set; }
        public int Partitions { get; set; }
    }

    // 3. Delete Topic
    public class DeleteTopic : IClusterCommand
    {
        public string CommandType => "DeleteTopic";
        public string Name { get; set; }
    }

    // 4. Register Broker
    public class RegisterBroker : IClusterCommand
    {
        public string CommandType => "RegisterBroker";
        public int Id { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
    }

    // 5. Alter topic config
    public class AlterTopicConfig : IClusterCommand
    {
        public string CommandType => "AlterTopicConfig";

        public string Key { get; set; }

        public string Value {get;set;}

    }
    // 6. Reassign partitions
    public class ReassignPartitions : IClusterCommand
    {
        public string CommandType => "ReassignPartitions";

        public string TopicName {get;set;}

        public List<PartitionMetadata> PartitionMetadata {get;set;} = new List<PartitionMetadata>();

    }

            public class PartitionMetadata
        {
            public int PartitionId {get;set;}
            public int BrokerId {get;set;}

        }

    
    // 7. Consumer Data
        public class ConsumerOffset : IClusterCommand
    {
        public string CommandType => "ConsumerOffset";
        public string Group { get; set; }
        public string TopicName { get; set; }
        public int PartitionId {get;set;}
        public long Offset {get;set;}

    }


    //-------------------------------- LOG ENTRY ----------------------------
    public class LogEntry
    {
        public int Term { get; set; }
        public IClusterCommand Command { get; set; } 
    }


}
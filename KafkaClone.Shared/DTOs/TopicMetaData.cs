using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaClone.Shared
{
    public class TopicMetaData
    {
        public required string TopicName {get;set;}
        public List<PartitionMetaData> PartitionMetadata = new();
    }

    public class PartitionMetaData
    {
        public int PartitionId {get;set;}
        public int BrokerId {get;set;}

    }
}




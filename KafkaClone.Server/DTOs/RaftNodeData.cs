using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaClone.Shared;

namespace KafkaClone.Server.DTOs
{
    public class RaftNodeData
    {
        public Broker BrokerIdentity {get;set;}
        public NodeState NodeState {get;set;}
        public int CurrentTerm {get;set;}
        public int? VotedFor {get;set;}
        public long LastIndex;
        public int LastTerm;
    }
}
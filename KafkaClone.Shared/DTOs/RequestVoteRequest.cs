using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaClone.Server
{
    public class RequestVoteRequest
    {
        public int BrokerId {get;set;}
        public int ElectionTerm {get;set;}
        public long LastLogIndex {get;set;}
        public int LastLogTerm {get;set;}

        public override string ToString()
        {
            return $"[RequestVote] Cand={BrokerId}, Term={ElectionTerm}, LastLog={LastLogIndex}(T{LastLogTerm})";
        }
    }
}
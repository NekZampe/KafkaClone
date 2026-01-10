using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaClone.Server.DTOs
{
    public class AppendEntriesResponse
    {
        public bool Success;
        public int Term;
        public long LastLogIndex;
        public long LastLogTerm;

        public override string ToString()
        {
            return $"[AppendEntriesResp] Success={Success}, Term={Term}, LastLog={LastLogIndex}";
        }
        
    }
}
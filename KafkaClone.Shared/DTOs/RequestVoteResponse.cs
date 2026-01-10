using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaClone.Server.DTOs
{
    public class RequestVoteResponse
    {
        public bool Verdict;
        public int CurrentTerm;

        public override string ToString()
        {
            return $"[VoteResp] {(Verdict ? "GRANTED" : "DENIED")}, Term={CurrentTerm}";
        }
        
    }
}
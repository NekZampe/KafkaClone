using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using KafkaClone.Server.DTOs;
using KafkaClone.Shared;

namespace KafkaClone.Server
{
    public interface IRaftTransport
    {
        public Task<RequestVoteResponse> SendRequestVoteRequest(RequestVoteRequest requestVoteRequest, Broker broker);
        public Task<AppendEntriesResponse> SendAppendEntriesRequest(AppendEntriesRequest request, Broker broker);
        public Task<AppendEntriesResponse> ForwardCommand(IClusterCommand request, Broker broker);
    }
}
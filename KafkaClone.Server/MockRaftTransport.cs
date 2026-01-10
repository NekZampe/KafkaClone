using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaClone.Server;
using KafkaClone.Server.DTOs;
using KafkaClone.Shared;

namespace KafkaClone.Server
{

public class MockRaftTransport : IRaftTransport
{
    // The "Network": Maps BrokerID -> Running Node Instance
    private readonly Dictionary<int, RaftNode> _nodes = new();
    
    // Simulates a cable cut (Network Partition)
    private readonly HashSet<int> _disconnectedNodes = new();

    public void RegisterNode(int id, RaftNode node) => _nodes[id] = node;

    // --- FAULT INJECTION METHODS ---
    public void IsolateNode(int id) => _disconnectedNodes.Add(id);
    public void ReconnectNode(int id) => _disconnectedNodes.Remove(id);

    // --- INTERFACE IMPLEMENTATION ---

    public async Task<AppendEntriesResponse> SendAppendEntriesRequest(AppendEntriesRequest request, Broker target)
    {
        // 1. Check Network Failures
        if (_disconnectedNodes.Contains(target.Id) || _disconnectedNodes.Contains(request.LeaderId))
        {
            // Simulate timeout
            await Task.Delay(10); 
            throw new Exception("Network Timeout");
        }

        // 2. Simulate Latency (Raft relies on timing, so this is important)
        await Task.Delay(5); 

        // 3. Direct Method Call
        var targetNode = _nodes[target.Id];
        return await targetNode.HandleAppendEntries(request);
    }

    public async Task<RequestVoteResponse> SendRequestVoteRequest(RequestVoteRequest request, Broker target)
    {
        if (_disconnectedNodes.Contains(target.Id) || _disconnectedNodes.Contains(request.BrokerId))
        {
            await Task.Delay(10);
            throw new Exception("Network Timeout");
        }

        await Task.Delay(5);
        
        var targetNode = _nodes[target.Id];
        return await targetNode.HandleRequestVote(request);
    }

    public async Task<ForwardCommandResponse> ForwardCommand(IClusterCommand command, Broker target)
    {
        if (_disconnectedNodes.Contains(target.Id)) 
            return new ForwardCommandResponse { Success = false, ErrorMessage = "Network Error" };

        var targetNode = _nodes[target.Id];
        return await targetNode.Propose(command);
    }
}
}
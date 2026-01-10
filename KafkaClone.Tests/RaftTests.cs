using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using KafkaClone.Server;
using KafkaClone.Server.DTOs;
using KafkaClone.Shared;

namespace KafkaClone.Tests
{

public class RaftTests
{
    [Fact(Skip = "Temporarily disabled")]
    public async Task Cluster_Elects_A_Single_Leader()
    {
        using var cluster = new RaftTestCluster();
        await cluster.Setup(3);

        // 1. Wait for election (Random timeout 150-300ms)
        await Task.Delay(1000);

        // 2. Verify State
        var leaders = cluster.Nodes.Where(n => n.State == NodeState.Leader).ToList();
        var followers = cluster.Nodes.Where(n => n.State == NodeState.Follower).ToList();

        Assert.Single(leaders); // Must be exactly 1 leader
        Assert.Equal(2, followers.Count); // Others must be followers
        Assert.Equal(leaders[0].CurrentTerm, followers[0].CurrentTerm); // Terms must match
    }

    
    [Fact]
    public async Task Data_Replicates_To_Majority()
    {
        using var cluster = new RaftTestCluster();
        await cluster.Setup(3);

        await WaitForCondition(() => cluster.Nodes.Any(n => n.State == NodeState.Leader), 5000);

        var leader = cluster.Nodes.First(n => n.State == NodeState.Leader);

        // 1. Propose Data
        var command = new CreateTopic { Name = "test-topic", Partitions = 3 };
        var result = await leader.Propose(command);
        
        await WaitForCondition(() => 
    {
        // Check if ANY follower has the log
        return cluster.Nodes.Any(n => n.Id != leader.Id && n.LastLogIndex > 0);
    }, 2000);

        // 2. Assert Success (Leader says "Committed")
        Assert.True(result.Success);

        // 3. Verify Logs on Followers
        // We wait briefly for the heartbeat to carry the data
        await Task.Delay(200);

        foreach (var node in cluster.Nodes)
        {
            // All nodes should have advanced their commit index
            Assert.True(node.LeaderCommit >= 1);
            
        }
    }

    [Fact(Skip = "Temporarily disabled")]
    public async Task Cluster_Survives_Leader_Crash()
    {
        using var cluster = new RaftTestCluster();
        await cluster.Setup(1);
        await Task.Delay(500);

        // 1. Identify Leader
        var oldLeader = cluster.Nodes.First(n => n.State == NodeState.Leader);
        int oldTerm = oldLeader.CurrentTerm;

        // 2. "Kill" the Leader (Network Isolation)
        cluster.Transport.IsolateNode(oldLeader.GetBrokerList()[0].Id); // Assuming GetBrokerList returns self id or similar logic

        // 3. Wait for new election
        await Task.Delay(1000);

        // 4. Find NEW Leader
        var newLeader = cluster.Nodes.FirstOrDefault(n => 
            n.State == NodeState.Leader && 
            n != oldLeader);

        Assert.NotNull(newLeader);
        Assert.True(newLeader.CurrentTerm > oldTerm); // Term increased
    }



    private async Task WaitForCondition(Func<bool> condition, int timeoutMs = 1000)
{
    var start = DateTime.Now;
    while ((DateTime.Now - start).TotalMilliseconds < timeoutMs)
    {
        if (condition()) return; // Success!
        await Task.Delay(50); // Check again in 50ms
    }
    // If we get here, we timed out. Let the Assertion fail naturally.
}

}
}
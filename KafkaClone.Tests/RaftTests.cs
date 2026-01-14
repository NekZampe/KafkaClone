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
        // 1. Setup
        using var cluster = new RaftTestCluster();
        await cluster.Setup(3);

        // Wait for a leader to be elected
        await WaitForCondition(() => cluster.Nodes.Any(n => n.State == NodeState.Leader), 5000, "Leader Election");
        var leader = cluster.Nodes.First(n => n.State == NodeState.Leader);

        // 2. Propose Data
        // We send 3 commands. In 0-based indexing:
        // Cmd 1 -> Index 0
        // Cmd 2 -> Index 1
        // Cmd 3 -> Index 2
        var commands = new List<CreateTopic>
        {
            new CreateTopic { Name = "test-topic1", Partitions = 3 },
            new CreateTopic { Name = "test-topic2", Partitions = 2 },
            new CreateTopic { Name = "test-topic3", Partitions = 6 }
        };

        foreach (var cmd in commands)
        {
            var result = await leader.Propose(cmd);
            Assert.True(result.Success, $"Leader failed to commit command {cmd.Name}: {result.ErrorMessage}");
        }

        long expectedLastIndex = 2; // Indices 0, 1, 2

        // 3. Verify Replication (Deterministic Polling)
        // We wait until a MAJORITY of nodes (2 out of 3) have:
        //  a) Appended the logs (LastLogIndex >= 2)
        //  b) Committed the logs (LeaderCommit >= 2)
        await WaitForCondition(() => 
        {
            int synchronizedNodes = cluster.Nodes.Count(n => 
                n.LastLogIndex >= expectedLastIndex && 
                n.LeaderCommit >= expectedLastIndex
            );
            return synchronizedNodes >= 2; // Majority for a 3-node cluster
        }, 5000, "Majority Replication");

        // 4. Deep Content Verification
        // Check that the logs on the followers actually contain the correct data
        int verifiedFollowers = 0;
        foreach (var node in cluster.Nodes)
        {
            // Only verify nodes that have actually caught up
            if (node.LastLogIndex < expectedLastIndex) continue;

            // Fetch Entry at Index 0 (Should be "test-topic1")
            // GetLogEntries(0, 0) fetches 1 item at index 0
            var logs = await node.GetLogEntries(0, 0); 
            Assert.Single(logs);
            
            var entry = logs[0];
            var command = entry.Command as CreateTopic;

            Assert.NotNull(command);
            Assert.Equal("test-topic1", command.Name);
            
            verifiedFollowers++;
        }

        Assert.True(verifiedFollowers >= 2, "At least a majority of nodes should have verified log content");
    }

    // ---------------------------------------------------------
    // Helper: Robust Polling Wrapper
    // ---------------------------------------------------------
    private async Task WaitForCondition(Func<bool> condition, int timeoutMs, string description)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(50); // Small poll interval
        }
        throw new TimeoutException($"Timed out waiting for: {description}");
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
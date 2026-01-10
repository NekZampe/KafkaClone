using KafkaClone.Server;
using KafkaClone.Server.DTOs;
using KafkaClone.Shared;
using KafkaClone.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace KafkaClone.Tests
{
    // ==================================================================================
    // 1. THE FAKE NETWORK (MockTransport)
    // ==================================================================================
    public class MockRaftTransport : IRaftTransport
    {
        private readonly Dictionary<int, RaftNode> _nodes = new();
        private readonly HashSet<int> _disconnectedNodes = new();

        public void RegisterNode(int id, RaftNode node) => _nodes[id] = node;

        // --- FAULT INJECTION ---
        public void IsolateNode(int id) => _disconnectedNodes.Add(id);
        public void ReconnectNode(int id) => _disconnectedNodes.Remove(id);

        // --- INTERFACE IMPLEMENTATION ---
        public async Task<AppendEntriesResponse> SendAppendEntriesRequest(AppendEntriesRequest request, Broker broker)
        {
            // 1. Simulate Network Delay (Optional but good for tests)
            await Task.Delay(10); 

            if (!_nodes.ContainsKey(broker.Id))
            {
                throw new Exception($"Network Error: Node {broker.Id} is unreachable.");
            }

            var node = _nodes[broker.Id];

            // 2. Call the node directly (await it!)
            return await node.HandleAppendEntries(request);
        }

        public async Task<RequestVoteResponse> SendRequestVoteRequest(RequestVoteRequest request, Broker target)
        {
            if (_disconnectedNodes.Contains(target.Id) || _disconnectedNodes.Contains(request.BrokerId))
            {
                await Task.Delay(10);
                throw new Exception("Network Timeout");
            }

            await Task.Delay(5);
            return await _nodes[target.Id].HandleRequestVote(request);
        }

        public async Task<ForwardCommandResponse> ForwardCommand(IClusterCommand command, Broker target)
        {
            if (_disconnectedNodes.Contains(target.Id)) 
                return new ForwardCommandResponse { Success = false, ErrorMessage = "Network Error" };

            return await _nodes[target.Id].Propose(command);
        }

        
    }

    // ==================================================================================
    // 2. THE TEST CLUSTER (RaftTestCluster)
    // ==================================================================================
    public class RaftTestCluster : IDisposable
    {
        public List<RaftNode> Nodes { get; } = new();
        public MockRaftTransport Transport { get; } = new();
        private readonly System.Collections.Concurrent.ConcurrentBag<string> _tempPaths = new();

 public async Task Setup(int nodeCount)
        {
            // 1. Create Broker Definitions
            var brokers = new List<Broker>();
            for (int i = 0; i < nodeCount; i++)
            {
                int tcpPort = 5000 + i;
                int grpcPort = 6000 + i;

                Console.WriteLine($"[TEST_CLUSTER] Adding new broker: id: {i}, Ports : {tcpPort}/{grpcPort}");
                brokers.Add(new Broker(i, tcpPort, grpcPort, "localhost"));
            }

            // 2. Initialize Nodes in Parallel (Simulate separate processes starting at once)
            var startupTasks = new List<Task<RaftNode>>();

            for (int i = 0; i < nodeCount; i++)
            {
                // Capture the index 'i' in a local variable to avoid closure issues in the loop
                int index = i;

                // Fire off a background task for this node's setup
                var task = Task.Run(async () =>
                {
                    string path = Path.Combine(Path.GetTempPath(), $"raft_test_{Guid.NewGuid()}");
                    _tempPaths.Add(path);
                    Directory.CreateDirectory(path);

                    // Filter out self from the peer list
                    var peerBrokers = brokers.Where(b => b.Id != index).ToList();

                    var clusterState = await ClusterState.InitializeAsync(path);

                    // Create the Node
                    var node = await RaftNode.InitializeNode(
                        path,
                        brokers[index], // Identity
                        NullLogger<Partition>.Instance,
                        Transport,
                        clusterState,
                        peerBrokers
                    );

                    // Thread-safe registration since multiple tasks hit this at once
                    lock (Transport)
                    {
                        Transport.RegisterNode(brokers[index].Id, node);
                    }
                    
                    Console.WriteLine($"[TEST_CLUSTER] Node {index} initialized and registered.");
                    return node;
                });

                startupTasks.Add(task);
            }

            // 3. Wait for ALL nodes to be fully up and running
            var initializedNodes = await Task.WhenAll(startupTasks);

            // Add them to our local list for cleanup later
            Nodes.AddRange(initializedNodes);
        }

        public void Dispose()
{
    // 1. FIRST: Kill the nodes to release file locks
    foreach (var raftnode in Nodes)
    {
        raftnode.Dispose();
    }

    System.Threading.Thread.Sleep(50); 

    // 2. SECOND: Now it is safe to delete the directories
    foreach (var path in _tempPaths)
    {
        try 
        {
            if (Directory.Exists(path)) 
            {
                Directory.Delete(path, true);
            }
        }
        catch (IOException ex)
        {
            // Optional: Log this, but don't fail the test if cleanup fails.
            // It just means a temp file is left over in %TEMP%
            Console.WriteLine($"[WARNING] Could not delete temp path {path}: {ex.Message}");
        }
    }
}
    }
}
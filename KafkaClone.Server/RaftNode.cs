using KafkaClone.Shared;

namespace KafkaClone.Server;

public enum NodeState
{
    Follower,
    Candidate,
    Leader
}

public class RaftNode
{
    private readonly Broker _myIdentity;
    
    // The current state (starts as Follower)
    public NodeState State { get; private set; } = NodeState.Follower;

    // We need a list of peers to ask for votes!
    private readonly List<Broker> _clusterMembers;

    public RaftNode(Broker identity, List<Broker> clusterMembers)
    {
        _myIdentity = identity;
        _clusterMembers = clusterMembers;
    }
}
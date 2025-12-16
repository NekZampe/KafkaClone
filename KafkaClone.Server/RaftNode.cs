using System.Runtime.CompilerServices;
using KafkaClone.Shared;
using KafkaClone.Server.DTOs;

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

    public int? VotedFor { get; private set; }

    public int CurrentTerm { get; private set; }

    private const int MinElectionTimeout = 150;

    private const int MaxElectionTimeout = 300;

    private static readonly Random _rng = new Random();

    private int NextRandomNumber => _rng.Next(MinElectionTimeout,MaxElectionTimeout);

    private CancellationTokenSource _electionTimeoutCts;

    private IRaftTransport _raftTransport;

    private long _lastIncludedIndex; // From the latest snapshot
    private int _lastIncludedTerm;   // From the latest snapshot

    private List<LogEntry> _logEntries;

    public long LastLogIndex => _logEntries.Count > 0 
    ? _lastIncludedIndex + _logEntries.Count 
    : _lastIncludedIndex;

    public long LastLogTerm => _logEntries.Count > 0 
    ? _logEntries.Last().Term 
    : _lastIncludedTerm;

    private RaftNode(Broker identity, List<Broker> clusterMembers,IRaftTransport raftTransport)
    {
        _myIdentity = identity;
        _clusterMembers = clusterMembers;
        _raftTransport = raftTransport;
        _logEntries =  new List<LogEntry>();
        _electionTimeoutCts = new CancellationTokenSource();

    }


    public async Task<RaftNode> InitializeNode()
    {
        
    }


    private async Task SaveState()
    {
        private readonly string _topicJsonPath;
        

    }




    private async Task ResetElectionTimer()
    {
        // Cancel old one
        if (_electionTimeoutCts is not null)
        {
            _electionTimeoutCts.Cancel();
            _electionTimeoutCts.Dispose();
        }

        _electionTimeoutCts = new CancellationTokenSource();
        int timeout = NextRandomNumber;

        _ = Task.Delay(timeout, _electionTimeoutCts.Token).ContinueWith(async t =>
        {
            if (!t.IsCanceled)
            {   
                State = NodeState.Candidate;
                CurrentTerm += 1;
                await StartElection();
                _ = ResetElectionTimer();
            } 

        });
    }

    private async Task StartElection()
    {
        if (_clusterMembers.Count < 1) 
            throw new Exception("Cluster cannot be empty");

        var ballot = new RequestVoteRequest
        {
            BrokerId = _myIdentity.Id,
            ElectionTerm = CurrentTerm
        };
        
        // Create a List of tasks to run them in parallel
        List<Task<RequestVoteResponse>> electionResults =  new List<Task<RequestVoteResponse>>();

        // store self votes ( start at one bc you vote for yourself )
        int counter = 1;

        // Send your vote to all
        foreach(var member in _clusterMembers)
        {
            electionResults.Add(_raftTransport.SendRequestVoteRequest(ballot,member));
        }

        // Find majority votes value needed to win
        int totalNodes = _clusterMembers.Count + 1;
        int majority = (totalNodes / 2) + 1;

        // Process responses
        while (electionResults.Count > 0)
        {
            // Wait for the next vote to arrive
            var completedTask = await Task.WhenAny(electionResults);
            electionResults.Remove(completedTask);

            var response = await completedTask;

            // If a peer has a higher term, we must step down immediately
            if ( response.CurrentTerm > CurrentTerm)
            {
                BecomeFollower(response.CurrentTerm);
                return;
            }
            // Count the vote if it was granted
            if ( response.Verdict)
             counter++;

            // Check for victory immediately after counting
            if ( counter >= majority)
            {
                DeclareVictory();
                return;
            } 
        }

        return;
    }

    private void DeclareVictory()
    {
    }

    private void BecomeFollower(int currentTerm)
    {
        State = NodeState.Follower;
        CurrentTerm = currentTerm;
    }


    private async Task<RequestVoteResponse> HandleRequestVote(RequestVoteRequest request)
    {   
        // 1. Term Update Rule: If candidate's term is higher, update state and step down.
        if(request.ElectionTerm > CurrentTerm)
        {
            CurrentTerm = request.ElectionTerm;
            State = NodeState.Follower;
            VotedFor = null;
        }

        // 2. Old Term Rejection: Reject if candidate's term is still lower than ours.
        if (request.ElectionTerm < CurrentTerm)
            return Reject();

        // 3. Loyalty Check: Reject if we already voted for someone else in this term.
        if (VotedFor is not null && VotedFor != request.BrokerId)
            return Reject();

        // 4. Log Safety Check: Reject if the voter's log is more up-to-date.
        if (LastLogTerm > request.LastLogTerm || (LastLogTerm == request.LastLogTerm && LastLogIndex > request.LastLogIndex))
            return Reject();

        
        VotedFor = request.BrokerId;

         return new RequestVoteResponse
        {   
            Verdict = true,
            CurrentTerm = CurrentTerm
        };
    }

    private RequestVoteResponse Reject()
    {
         return new RequestVoteResponse
            {   
                Verdict = false,
                CurrentTerm = CurrentTerm

            };
    }



}
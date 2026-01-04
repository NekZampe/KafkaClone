using System.Runtime.CompilerServices;
using KafkaClone.Shared;
using KafkaClone.Server.DTOs;
using KafkaClone.Storage;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using System.ComponentModel;
using System.Threading.Tasks;
using System.Collections;

namespace KafkaClone.Server;

public enum NodeState
{
    Follower,
    Candidate,
    Leader
}

public class RaftNode
{
    private readonly string _basePath;
    private readonly Broker _myIdentity;

    private readonly ILogger<Partition> _logger;
    
    // The current state (starts as Follower)
    public NodeState State { get; private set; } = NodeState.Follower;

    // We need a list of peers to ask for votes
    private readonly List<Broker> _clusterMembers;

    public int? CurrentLeaderId { get; private set; }

    public int? VotedFor { get; private set; }

    public int CurrentTerm { get; private set; }

    private const int MinElectionTimeout = 150;

    private const int MaxElectionTimeout = 300;

    private static readonly Random _rng = new Random();

    private int NextRandomNumber => _rng.Next(MinElectionTimeout,MaxElectionTimeout);

    private CancellationTokenSource _electionTimeoutCts;

    private IRaftTransport _raftTransport;

    private readonly Partition _raftLog;

    public long LastLogIndex => _raftLog.CurrentOffset;

    public int LastLogTerm;

    public long LeaderCommit = 0; // Highest index of commited file

    private Dictionary<int,long> _nextIndex; // Used to find common anchor

    private Dictionary<int,long> _matchIndex; // Highest Index of follower commited

    private RaftNode(string basePath,Broker identity, List<Broker> clusterMembers,ILogger<Partition> logger,IRaftTransport raftTransport, Partition raftLog, RaftNodeData nodeData)
    {
        _basePath = basePath;
        _myIdentity = identity;
        _logger = logger;
        _clusterMembers = clusterMembers;
        _raftTransport = raftTransport;
        _electionTimeoutCts = new CancellationTokenSource();
        _raftLog = raftLog;

        // Apply RaftNodeState data
        this.CurrentTerm = nodeData.CurrentTerm;
        this.State = nodeData.NodeState;
        this.LastLogTerm = nodeData.LastTerm;
        this.LeaderCommit = nodeData.LeaderCommit;
        this.VotedFor = nodeData.VotedFor;

    }

    public static async Task<RaftNode> InitializeNode(
        string basePath, 
        Broker identity,
        ILogger<Partition> partitionLogger,
        List<Broker> clusterMembers, 
        IRaftTransport raftTransport)
    {
        RaftNodeState raftNodeState;
        string statePath = Path.Combine(basePath, "nodestate.json");

        if (!File.Exists(statePath))
        {
            // 1. Setup new persistent data
            var initialData = new RaftNodeData 
            { 
                BrokerIdentity = identity,
                NodeState = NodeState.Follower,
                CurrentTerm = 0,
                VotedFor = null,
                LastTerm = 0,
                LeaderCommit = 0
            };

            // 2. Persist it for the first time
            await RaftNodeState.CreateAsync(basePath, initialData);
            
            string newFolder = Path.Combine(basePath, "__nodeLogEntries__");

            // Create Partition. TODO - UPDATE BASE VALUES
            Partition partition = new Partition(0,newFolder,identity.Id, false, partitionLogger, TimeSpan.FromHours(5), 1024);

            
            return new RaftNode(basePath,identity,clusterMembers,partitionLogger,raftTransport,partition,initialData);
        }
            else
            {
                // 3.0 Define the path where logs are stored
                string logFolder = Path.Combine(basePath, "__nodeLogEntries__");

                // 3.1 Load existing persistent state (Term, VotedFor, etc.)
                raftNodeState = await RaftNodeState.LoadAsync(statePath) 
                            ?? throw new Exception("Failed to load existing state.");

                // 3.2 Initialize the partition. 
                // The constructor will automatically find and open existing .log and .index files.
                Partition partition = new Partition(
                    0, 
                    logFolder, 
                    identity.Id, 
                    false, 
                    partitionLogger, 
                    TimeSpan.FromHours(5), 
                    1024);

                // 3.3 Create the node instance with the loaded partition
                var raftNode = new RaftNode(
                    basePath,
                    raftNodeState.RaftNodeData.BrokerIdentity,
                    clusterMembers,
                    partitionLogger,
                    raftTransport,
                    partition,
                    raftNodeState.RaftNodeData);

                return raftNode;
            }
        
    }

    private async Task PersistStateAsync()
{
    var data = new RaftNodeData
    {
        BrokerIdentity = _myIdentity,
        CurrentTerm = this.CurrentTerm,
        VotedFor = this.VotedFor,
        NodeState = this.State,
        LastTerm = this.CurrentTerm
    };

    await RaftNodeState.SaveStateAsync(_basePath,data);
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
                await BecomeFollower(response.CurrentTerm);
                return;
            }
            // Count the vote if it was granted
            if ( response.Verdict)
             counter++;

            // Check for victory immediately after counting
            if ( counter >= majority)
            {
                await DeclareVictory();
                return;
            } 
        }

        return;
    }


    private async Task DeclareVictory()
    {
        
        State = NodeState.Leader;
        CurrentLeaderId = _myIdentity.Id;

        _nextIndex = new Dictionary<int, long>();
        _matchIndex = new Dictionary<int, long>();


        foreach( var member in _clusterMembers)
        {
            _nextIndex[member.Id] = LastLogIndex + 1;
            _matchIndex[member.Id] = 0;
        }
        //Start Heartbeat loop
        await StartHeartbeatLoop(_electionTimeoutCts.Token);

    }

    private async Task StartHeartbeatLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && State == NodeState.Leader)
        {
            await SendHeartbeats();
            await Task.Delay(50, ct); // Heartbeat interval (e.g., 50ms)
        }
    }

    private async Task SendHeartbeats()
    {

        foreach(var member in _clusterMembers)
        {
            var offset = _nextIndex[member.Id]-1;
            if ( offset < 0) offset = 0;

            var heartbeat =  new AppendEntriesRequest
        {
            Term = CurrentTerm,
            LeaderId = _myIdentity.Id,
            PrevLogIndex = offset,
            PrevLogTerm = await GetTermByIndex(offset),
            LeaderCommit = LastLogIndex,
            Entries = new List<LogEntry>()
            
        };
            var response = await _raftTransport.SendAppendEntriesRequest(heartbeat,member);

        }
    }

  
private async Task HandleHeartbeatResponse(AppendEntriesResponse response, Broker broker)
{
    // 1. Follower's term is higher so we step down
    if (response.Term > CurrentTerm)
    {
        await BecomeFollower(response.Term);
        return; // Stop processing after stepping down
    }

    // 2. Follower's Log index is behind
    if (!response.Success)
    {
        // Decrement nextIndex and retry
        if (_nextIndex[broker.Id] > 0)
        {
            _nextIndex[broker.Id]--;
        }
        
        // The next heartbeat will retry with the decremented index
        return;
    }

    // 3. Success case - update matchIndex and nextIndex
    if (response.Success)
    {
        // Update match and next indices
        _matchIndex[broker.Id] = response.LastLogIndex;
        _nextIndex[broker.Id] = response.LastLogIndex + 1;

        // 4. Update commit index based on majority replication
        await UpdateCommitIndex();
    }
}

private async Task UpdateCommitIndex()
{
    // Collect all match indices including our own log
    var matchIndices = new List<long> { LastLogIndex };
    
    foreach (var kvp in _matchIndex)
    {
        matchIndices.Add(kvp.Value);
    }

    matchIndices.Sort();
    matchIndices.Reverse(); // Sort in DESCENDING order
    
    // Find the median (majority) index
    int majorityIndex = matchIndices.Count / 2;
    long newCommitIndex = matchIndices[majorityIndex];

    // Only commit entries from current term (Raft safety requirement)
    if (newCommitIndex > LeaderCommit)
    {
        long termAtIndex = await GetTermByIndex(newCommitIndex);
        
        if (termAtIndex == CurrentTerm)
        {
            LeaderCommit = newCommitIndex;
            await PersistStateAsync();
        }
    }
}
// The Flow of Data
// Append: New commands are added to the end of the Log. (They are uncommitted).

// Replicate: We wait for a majority of nodes to have these entries.

// Commit: We update CommitIndex.

// Apply: This ApplyCommittedEntriesAsync method looks at the log and says, 
// "Oh, LastApplied is 5, but CommitIndex is 7. I need to apply entries 6 and 7."


    public async Task<bool> Propose(IClusterCommand command)
    {

        if (_myIdentity.Id != CurrentLeaderId)
        {
            await IRaftTransport.
        }

        var type = command.CommandType;

        switch (type)
        {
            case "CreateTopic":
            break;
            case "RegisterBroker":
            break;
            case "ConsumerOffset":
            break;
        }
        
    }





    private async Task BecomeFollower(int term)
    {
        State = NodeState.Follower;
        CurrentTerm = term;
        await PersistStateAsync();
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

        await PersistStateAsync();

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

    public async Task<AppendEntriesResponse> HandleAppendEntries(AppendEntriesRequest request)
    {
        // 1. Reply false if term < currentTerm
        if (request.Term < CurrentTerm)
        {
            return new AppendEntriesResponse
            {
                Success = false,
                Term = CurrentTerm,
                LastLogIndex = LastLogIndex
            };
        }

        // 2. If term is newer, step down
        if (request.Term > CurrentTerm)
        {
            CurrentTerm = request.Term;
            State = NodeState.Follower;
            VotedFor = null;
            await PersistStateAsync();
        }

        if (CurrentLeaderId != request.LeaderId)
        {
            CurrentLeaderId = request.LeaderId;
            await PersistStateAsync();
        }

        await ResetElectionTimer();

        // 3. Reply false if log doesn’t contain PrevLogIndex
        if (request.PrevLogIndex > LastLogIndex)
        {
            return new AppendEntriesResponse
            {
                Success = false,
                Term = CurrentTerm,
                LastLogIndex = LastLogIndex
            };
        }

        // 4. Reply false if term mismatch at PrevLogIndex
        if (request.PrevLogIndex >= 0)
        {
            byte[] anchorBytes = await _raftLog.ReadAsync(request.PrevLogIndex);
            LogEntry anchorEntry = DeserializeLogEntry(anchorBytes);

            if (anchorEntry.Term != request.PrevLogTerm)
            {
                return new AppendEntriesResponse
                {
                    Success = false,
                    Term = CurrentTerm,
                    LastLogIndex = LastLogIndex
                };
            }
        }

        // 5. Append / overwrite entries
        for (int i = 0; i < request.Entries.Count; i++)
        {
            var entry = request.Entries[i];
            long entryIndex = request.PrevLogIndex + 1 + i;

            if (entryIndex <= LastLogIndex)
            {
                byte[] localData = await _raftLog.ReadAsync(entryIndex);
                LogEntry localEntry = DeserializeLogEntry(localData);

                if (localEntry.Term != entry.Term)
                {
                    await _raftLog.TruncateFromIndexAsync(entryIndex);
                    await _raftLog.AppendAsync(SerializeLogEntry(entry));
                    LastLogTerm = entry.Term;
                }
            }
            else
            {
                await _raftLog.AppendAsync(SerializeLogEntry(entry));
            }
        }

        return new AppendEntriesResponse
        {
            Success = true,
            Term = CurrentTerm,
            LastLogIndex = LastLogIndex
        };
    }

        public byte[] SerializeLogEntry(LogEntry logEntry)
    {
        return JsonSerializer.SerializeToUtf8Bytes(logEntry);
    }

    public LogEntry DeserializeLogEntry(byte[] logEntryBytes)
    {
        return JsonSerializer.Deserialize<LogEntry>(logEntryBytes) 
            ?? throw new Exception("Failed to deserialize LogEntry.");
    }

    public async Task<int> GetLastTermAsync()
    {
        if (_raftLog is null || _raftLog.IndexLength == 0) return 0;

        byte[] lastLogBytes = await _raftLog.ReadAsync(LastLogIndex - 1);

        LogEntry logEntry = DeserializeLogEntry(lastLogBytes);

        if (logEntry is null) return 0;

        return logEntry.Term;
        
    }

    private async Task<int> GetTermByIndex(long index)
    {
        // Raft convention: the term at the very beginning of time is 0
        if (index <= 0) 
        {
            return 0; 
        }

        try 
        {
            byte[] logBytes = await _raftLog.ReadAsync(index);
            LogEntry log = DeserializeLogEntry(logBytes);
            return log.Term;
        }
        catch (Exception)
        {
            // If the log is shorter than we thought, we treat it as term 0
            return 0;
        }
    }


}
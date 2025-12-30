using System.Runtime.CompilerServices;
using KafkaClone.Shared;
using KafkaClone.Server.DTOs;
using KafkaClone.Storage;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using System.ComponentModel;
using System.Threading.Tasks;

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

    private Dictionary<int,long> _nextIndex;

    private Dictionary<int,long> _matchIndex;

    private RaftNode(string basePath,Broker identity, List<Broker> clusterMembers,ILogger<Partition> logger,IRaftTransport raftTransport, Partition raftLog)
    {
        _basePath = basePath;
        _myIdentity = identity;
        _logger = logger;
        _clusterMembers = clusterMembers;
        _raftTransport = raftTransport;
        _electionTimeoutCts = new CancellationTokenSource();
        _raftLog = raftLog;

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
                LastIndex = 0,
                LastTerm = 0
            };

            // 2. Persist it for the first time
            await RaftNodeState.CreateAsync(basePath, initialData);
            
            string newFolder = Path.Combine(basePath, "__nodeLogEntries__");

            // Create Partition. TODO - UPDATE BASE VALUES
            Partition partition = new Partition(0,newFolder,identity.Id, false, partitionLogger, TimeSpan.FromHours(5), 1024);

            

            return new RaftNode(basePath,identity,clusterMembers,partitionLogger,raftTransport,partition);
        }
            else
            {
                // 3.0 Define the path where logs are stored
                string logFolder = Path.Combine(basePath, "__nodeLogEntries__");

                // 3.1 Load existing persistent state (Term, VotedFor, etc.)
                raftNodeState = await RaftNodeState.LoadAsync(basePath) 
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
                    partition);

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
        LastIndex = this.LastLogIndex,
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


    private async Task DeclareVictory()
    {
        
        State = NodeState.Leader;

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
            
        }
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


    public async Task AppendEntries(AppendEntriesRequest request)
    {
        
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


}
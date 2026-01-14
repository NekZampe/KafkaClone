using System.Runtime.CompilerServices;
using KafkaClone.Shared;
using KafkaClone.Server.DTOs;
using KafkaClone.Storage;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using System.ComponentModel;
using System.Threading.Tasks;
using System.Collections;
using System.Security;
using KafkaClone.Storage.Contracts;

namespace KafkaClone.Server;

public enum NodeState
{
    Follower,
    Candidate,
    Leader
}

public class RaftNode : IDisposable
{
    // =========================
    // CORE IDENTITY & DEPENDENCIES
    // =========================
    private readonly string _basePath;
    private readonly Broker _myIdentity;
    private readonly ClusterState _clusterState;
    private readonly ILogger<Partition> _logger;
    private IRaftTransport _raftTransport;
    private readonly IPartition _raftLog;
    public int Id => _myIdentity.Id;

    // =========================
    // RAFT STATE
    // =========================
    public NodeState State { get; private set; } = NodeState.Follower;
    public int? CurrentLeaderId { get; private set; }
    public int? VotedFor { get; private set; }
    public int CurrentTerm { get; private set; }
    public int LastLogTerm;
    public long LeaderCommit = -1; // Highest index of commited file
    private volatile bool _disposed = false;

    // =========================
    // CLUSTER MEMBERSHIP
    // =========================
    private List<Broker> _clusterMembers;

    // =========================
    // LOG STATE
    // =========================
    public long LastLogIndex => _raftLog.CurrentOffset - 1;
    private long _lastApplied = -1;
    public long LastApplied => _lastApplied;

    // =========================
    // LEADER REPLICATION STATE
    // =========================
    private Dictionary<int,long> _nextIndex;   // Used to find common anchor
    private Dictionary<int,long> _matchIndex;  // Highest Index of follower commited

    // =========================
    // TIMING & ELECTION
    // =========================
    private const int MinElectionTimeout = 1000;
    private const int MaxElectionTimeout = 2000;
    private static readonly Random _rng = new Random();
    private int NextRandomNumber => _rng.Next(MinElectionTimeout,MaxElectionTimeout);
    private CancellationTokenSource _electionTimeoutCts;
    private CancellationTokenSource? _leaderStopCts;

    // =========================
    // CONCURRENCY & DISK SAFETY
    // =========================
    private readonly SemaphoreSlim _diskLock = new SemaphoreSlim(1, 1);


    // =========================
    // Initialization
    // =========================

    private RaftNode(string basePath,Broker identity, List<Broker> clusterMembers,ILogger<Partition> logger,IRaftTransport raftTransport, IPartition raftLog, RaftNodeData nodeData,ClusterState clusterState)
    {
        _basePath = basePath;
        _myIdentity = identity;
        _logger = logger;
        _clusterMembers = clusterMembers;
        _raftTransport = raftTransport;
        _electionTimeoutCts = new CancellationTokenSource();
        _raftLog = raftLog;
        _clusterState = clusterState;

        // Apply RaftNodeState data
        this.CurrentTerm = nodeData.CurrentTerm;
        this.State = nodeData.NodeState;
        this.LastLogTerm = nodeData.LastTerm;
        this.LeaderCommit = nodeData.LeaderCommit;
        this.VotedFor = nodeData.VotedFor;
        

    }

    public void Dispose()
        {
            _disposed = true;

            try 
            {
                _electionTimeoutCts?.Cancel(); 
                _leaderStopCts?.Cancel();

                _electionTimeoutCts?.Dispose();
                _leaderStopCts?.Dispose();
            } 
            catch (ObjectDisposedException) { }

            if (_raftLog is IDisposable disposableLog)
            {
                disposableLog.Dispose();
            }

            // 3. Clean up the semaphore
            _diskLock?.Dispose();
        }

    // =========================
    // NODE BOOTSTRAP
    // =========================

    public static async Task<RaftNode> InitializeNode(
        string basePath, 
        Broker identity,
        ILogger<Partition> partitionLogger, 
        IRaftTransport raftTransport,
        ClusterState clusterState,
        List<Broker> bootstrapClusterMembers,
        IPartition partition)
    {
        var clusterMembers = bootstrapClusterMembers;
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
                LeaderCommit = -1
            };

            // 2. Persist it for the first time
            await RaftNodeState.CreateAsync(basePath, initialData);
            
            var raftNode = new RaftNode(basePath,identity,clusterMembers,partitionLogger,raftTransport,partition,initialData,clusterState);
             _ = raftNode.ResetElectionTimer();
        
            return raftNode;
        }
            else
            {

                // 3.1 Load existing persistent state (Term, VotedFor, etc.)
                raftNodeState = await RaftNodeState.LoadAsync(statePath) 
                            ?? throw new Exception("Failed to load existing state.");

                // 3.3 Create the node instance with the loaded partition
                var raftNode = new RaftNode(
                    basePath,
                    raftNodeState.RaftNodeData.BrokerIdentity,
                    clusterMembers,
                    partitionLogger,
                    raftTransport,
                    partition,
                    raftNodeState.RaftNodeData,
                    clusterState);

                await raftNode.ReplayLog();

                 _ = raftNode.ResetElectionTimer();
        
                return raftNode;
            }
        
    }


        private async Task ReplayLog()
{
    _logger.LogInformation("Replaying Raft Log to restore Cluster State...");
    
     _lastApplied = -1;

    // 1. Iterate through the entire log from the beginning
    long logSize = _raftLog.CurrentOffset; 
    
    for (long index = 0; index < logSize; index++)
    {
        try 
        {
            // 2. Read Raw Bytes
            byte[] entryBytes = await _raftLog.ReadAsync(index);
            
            // 3. Deserialize 
            LogEntry entry = DeserializeLogEntry(entryBytes);

            // 4. Update the "Brain"
            if (entry.Command != null)
            {
                _clusterState.ApplyCommand(entry.Command);
            }
            
            // 5. Update Local State logic
            if (entry.Term > this.CurrentTerm)
            {
                this.CurrentTerm = entry.Term;
            }

            _lastApplied = index;
        }
        catch (Exception ex)
        {
            _logger.LogError($"Failed to replay log at index {index}: {ex.Message}");
            
        }
    }

    // Sync our "Applied" counter to the end of the log
    _logger.LogInformation($"Replay complete. Last Applied Index: {_lastApplied}");
}

    // =========================
    // PERSISTENT STATE
    // =========================
    private async Task PersistStateAsync()
        {
            var data = new RaftNodeData
            {
                BrokerIdentity = _myIdentity,
                CurrentTerm = this.CurrentTerm,
                VotedFor = this.VotedFor,
                NodeState = this.State,
                LastTerm = this.CurrentTerm,
                LeaderCommit = this.LeaderCommit
            };

            // 2. Wrap the save in a Try/Finally Lock
            await _diskLock.WaitAsync();
            try
            {
                await RaftNodeState.SaveStateAsync(_basePath, data);
            }
            finally
            {
                _diskLock.Release();
            }
        }

    // =========================
    // ELECTION LOGIC
    // =========================
    private async Task StartElection()
    {
        if (_clusterMembers.Count < 1) 
            throw new Exception("Cluster cannot be empty");

        var ballot = new RequestVoteRequest
        {
            BrokerId = _myIdentity.Id,
            ElectionTerm = CurrentTerm,
            LastLogIndex = this.LastLogIndex,  
            LastLogTerm = await GetLastTermAsync()
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

private async Task ResetElectionTimer()
{
    // 1. STOP if the node is shutting down 
    if (_disposed) return; 

    // 2. Cancel the old timer safely
    if (_electionTimeoutCts is not null)
    {
        try 
        {
            if (!_electionTimeoutCts.IsCancellationRequested)
            {
                _electionTimeoutCts.Cancel();
            }
        }
        catch (ObjectDisposedException)
        {
        }
        finally
        {
            // Always ensure the old handle is cleaned up
            _electionTimeoutCts.Dispose();
        }
    }

    // 3. Create the new timer
    _electionTimeoutCts = new CancellationTokenSource();
    int timeout = NextRandomNumber;

    // 4. Run the election loop
    var tokenToUse = _electionTimeoutCts.Token;

    _ = Task.Delay(timeout, tokenToUse).ContinueWith(async t =>
    {
        // Only start election if WE weren't cancelled
        if (!t.IsCanceled && !tokenToUse.IsCancellationRequested && !_disposed)
        {   
            // Double check state before starting election
            if (State != NodeState.Leader) 
            {
                State = NodeState.Candidate;
                CurrentTerm += 1;
                await StartElection();
                
                if (State != NodeState.Leader) 
                {
                    await ResetElectionTimer();
                }
            }
        } 
    }, TaskScheduler.Default);
}

    // =========================
    // LEADER LOGIC
    // =========================
    private async Task DeclareVictory()
    {

        Console.WriteLine($"[DeclareVictory] Broker-{Id} Declaring Victory!");
        
        State = NodeState.Leader;
        CurrentLeaderId = _myIdentity.Id;

        _logger.LogInformation($"[RAFT] Broker {_myIdentity.Id} elected as LEADER for term {CurrentTerm}");

        _nextIndex = new Dictionary<int, long>();
        _matchIndex = new Dictionary<int, long>();


        foreach( var member in _clusterMembers)
        {
            _nextIndex[member.Id] = LastLogIndex + 1;
            _matchIndex[member.Id] = -1;
        }

        _matchIndex[_myIdentity.Id] = LastLogIndex;
        _nextIndex[_myIdentity.Id] = LastLogIndex + 1;

            // 2. STOP the old election timer (we won, we don't need to vote anymore)
            _electionTimeoutCts?.Cancel();
            _electionTimeoutCts?.Dispose();
            _electionTimeoutCts = null; // Clear it so we don't accidentally use it

            // 3. CREATE a new token specifically for this leadership term
            _leaderStopCts = new CancellationTokenSource();

            // 4. Start the loop with the NEW valid token
            try 
            {
                await StartHeartbeatLoop(_leaderStopCts.Token);
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown when stepping down
            }
    }


    private async Task BecomeFollower(int term)
    {
        // 1. If we were Leader, stop the heartbeat loop now!
        if (State == NodeState.Leader)
        {
            _leaderStopCts?.Cancel();
            _leaderStopCts?.Dispose();
            _leaderStopCts = null;
        }

        State = NodeState.Follower;
        CurrentTerm = term;
        
        await ResetElectionTimer();

        await PersistStateAsync();
    }

    private async Task StartHeartbeatLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && State == NodeState.Leader)
        {
            _logger.LogDebug($"[RAFT] {_myIdentity.Id} Sending HeartBeats...");
            await SendHeartbeats();
            await Task.Delay(50, ct); // Heartbeat interval (e.g., 50ms)
        }
    }


    // =========================
    // HEARTBEATS & REPLICATION
    // =========================
private async Task SendHeartbeats()
{
    Console.WriteLine($"[SendHeartBeats] Broker-{Id}: Sending Heartbeats...");
    var tasks = new List<Task>();

    foreach (var member in _clusterMembers)
    {
        // Capture member variable for the closure
        var broker = member; 
        
        tasks.Add(Task.Run(async () => 
        {
            try 
            {
                long nextIdx = _nextIndex[broker.Id];
                Console.WriteLine($"[Debug] Broker-{Id}: broker-{broker.Id}'s nextIdx:{nextIdx}");
                var entriesToSend = new List<LogEntry>();

                // Fetch entries if the follower is behind
                if (LastLogIndex >= nextIdx)
                {
                    Console.WriteLine($"[SendHeartBeats] Broker:{Id} fetching past log entries for slow follower [nextIdx:{nextIdx},LastLogIndex:{LastLogIndex}]");
                    entriesToSend = await GetLogEntries(nextIdx, LastLogIndex);  // NOT LOGGED
                }

                var prevLogIndex = nextIdx - 1;
                // Safety check for beginning of log
                if (prevLogIndex < 0) prevLogIndex = 0; 

                var heartbeat = new AppendEntriesRequest
                {
                    Term = CurrentTerm,
                    LeaderId = _myIdentity.Id,
                    PrevLogIndex = prevLogIndex,
                    PrevLogTerm = await GetTermByIndex(prevLogIndex),
                    LeaderCommit = LeaderCommit,
                    Entries = entriesToSend
                };
                
                Console.WriteLine($"[SendHeartBeats] Broker:{Id}-> Sending Heartbeat to Broker:{broker.Id} -> data:{heartbeat.ToString()}"); //NOT LOGGED

                var response = await _raftTransport.SendAppendEntriesRequest(heartbeat, broker);

                Console.WriteLine($"[SendHeartBeats] Broker:{Id}-> response received from Broker:{broker.Id} -> response:{response.ToString()}");

                await HandleHeartbeatResponse(response, broker);

            }
            catch (Exception ex)
            {
                // Log failure to reach this specific node, don't crash loop
                Console.WriteLine($"Failed to contact {broker.Id}: {ex.Message}");
            }
        }));
    }

    // Send to all nodes in parallel
    await Task.WhenAll(tasks);
}

private async Task HandleHeartbeatResponse(AppendEntriesResponse response, Broker broker)
{
    // 1. Follower's term is higher so we step down
    if (response.Term > CurrentTerm)
    {
         _logger.LogInformation($"[RAFT] Broker {_myIdentity.Id} stepping down for term {CurrentTerm}");
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
        _nextIndex[broker.Id] = response.LastLogIndex + 1;
        _matchIndex[broker.Id] = response.LastLogIndex;

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
            await ApplyCommittedEntries();
            await PersistStateAsync();
        }
    }

    Console.WriteLine($"[DEBUG] Broker:{Id} New Commit index: {LeaderCommit}");
}
    // =========================
    // CLIENT COMMAND HANDLING
    // =========================

public async Task<ForwardCommandResponse> Propose(IClusterCommand command)
{
    Console.WriteLine($"[m-Propose] Broker:{_myIdentity.Id} About to propose new entry");
    
    // 1. Forwarding Logic - if we're not the leader, forward to leader
    if (_myIdentity.Id != CurrentLeaderId)
    {
        var leaderBroker = _clusterMembers.FirstOrDefault(broker => broker.Id == CurrentLeaderId);
        if (leaderBroker == null) 
        {
            return new ForwardCommandResponse
            {
                Success = false,
                ErrorMessage = "No Leader Found"
            };
        }
        
        var response = await _raftTransport.ForwardCommand(command, leaderBroker); 
        return response;
    }

    // 2. Leader Logic: Capture the index we're proposing BEFORE appending
    long proposedIndex = LastLogIndex + 1;
    
    var newEntry = new LogEntry
    {
        Term = CurrentTerm,
        Index = proposedIndex,
        Command = command
    };
    
    Console.WriteLine($"[m-Propose] Broker:{_myIdentity.Id} About to append LogEntry at index {proposedIndex}");
    
    // 3. Append to our local log
    await _raftLog.AppendAsync(SerializeLogEntry(newEntry));

    Console.WriteLine($"[DEBUG] After Append, LastLogIndex is: {LastLogIndex}");
    try 
    {
        byte[] readBack = await _raftLog.ReadAsync(proposedIndex);
        var readEntry = DeserializeLogEntry(readBack);
        Console.WriteLine($"[DEBUG] Successfully read back entry at index {proposedIndex}: {readEntry?.Index}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[DEBUG] FAILED to read back entry at index {proposedIndex}: {ex.Message}");
    }

    Console.WriteLine($"[DEBUG] After Append, LastLogIndex is: {LastLogIndex}");
    
    // 4. Update our own replication indices
    _matchIndex[_myIdentity.Id] = LastLogIndex; 
    _nextIndex[_myIdentity.Id] = LastLogIndex + 1;

    // 5. Trigger replication to followers (fire and forget)
    _ = SendHeartbeats(); 
    
    // 6. Wait for the entry to be committed (with timeout)
    var timeout = TimeSpan.FromSeconds(5);
    var deadline = DateTime.UtcNow + timeout;
    
    while (LeaderCommit < proposedIndex && DateTime.UtcNow < deadline)
    {
        await Task.Delay(10);
        
        // Additional check: if we're no longer leader, abort
        if (State != NodeState.Leader)
        {
            return new ForwardCommandResponse 
            { 
                Success = false, 
                ErrorMessage = "Lost leadership while waiting for commit" 
            };
        }
    }
    
    // 7. Check if we successfully committed
    if (LeaderCommit >= proposedIndex)
    {
        return new ForwardCommandResponse 
        { 
            Success = true,
            ErrorMessage = null
        };
    }
    else
    {
        return new ForwardCommandResponse 
        { 
            Success = false, 
            ErrorMessage = "Timeout waiting for commit - entry may still be replicated" 
        };
    }
}


    // =========================
    // LOG ACCESS HELPERS
    // =========================
public async Task<List<LogEntry>> GetLogEntries(long startIndex, long endIndex)
{

    int count = (int) (endIndex - startIndex) + 1;
    if (count <= 0) return new List<LogEntry>();
    var list = new List<LogEntry>();

    var result = await _raftLog.ReadBatchAsync(startIndex,count);


    foreach(var logBytes in result.Messages)
    {
        list.Add(DeserializeLogEntry(logBytes));
    }

    Console.WriteLine($"[GetLogEntries] Number of Log entries retrieved: {list.Count()}");

    return list;
}

    // =========================
    // REQUEST VOTE RPC
    // =========================
    public async Task<RequestVoteResponse> HandleRequestVote(RequestVoteRequest request)
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

    // =========================
    // APPEND ENTRIES RPC
    // =========================

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

        if (request.PrevLogIndex > 0)
{
        // 3. Reply false if log doesn’t contain PrevLogIndex
        if (request.PrevLogIndex > LastLogIndex)
        {
             _logger.LogWarning($"PrevLogIndex {request.PrevLogIndex} is beyond our log (LastLogIndex: {LastLogIndex})");
            return new AppendEntriesResponse
            {
                Success = false,
                Term = CurrentTerm,
                LastLogIndex = LastLogIndex
            };
        }

        try
            {
                byte[] anchorBytes = await _raftLog.ReadAsync(request.PrevLogIndex);
                LogEntry anchorEntry = DeserializeLogEntry(anchorBytes);

                if(anchorEntry is null) return new AppendEntriesResponse
                {
                    Success = false,
                    Term = CurrentTerm,
                    LastLogIndex = LastLogIndex
                };

                if (anchorEntry.Term != request.PrevLogTerm)
                {
                    _logger.LogWarning($"Term mismatch at PrevLogIndex {request.PrevLogIndex}");
                    return new AppendEntriesResponse
                    {
                        Success = false,
                        Term = CurrentTerm,
                        LastLogIndex = LastLogIndex
                    };
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error reading log at PrevLogIndex {request.PrevLogIndex}: {ex.Message}");
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

        if (request.Entries.Count > 0)
        {
            LastLogTerm = request.Entries[^1].Term;
        }

        // 6. Update Commit Index
        if (request.LeaderCommit > LeaderCommit)
        {
            // We can only commit up to what we actually have in our log
            LeaderCommit = Math.Min(request.LeaderCommit, LastLogIndex);
            
            // Optional: Trigger state machine application here (e.g. ApplyToStateMachine())
            _logger.LogInformation($"[RAFT] Node {_myIdentity.Id} advanced CommitIndex to {LeaderCommit}");
            
            await ApplyCommittedEntries();
        }

        return new AppendEntriesResponse
        {
            Success = true,
            Term = CurrentTerm,
            LastLogIndex = LastLogIndex
        };
    }

        public async Task UpdateLastApplied(long index)
    {
        _lastApplied = index;
    }

    private async Task ApplyCommittedEntries()
{
    while (_lastApplied < LeaderCommit)
    {
        _lastApplied++;
        
        try
        {
            byte[] entryBytes = await _raftLog.ReadAsync(_lastApplied);
            LogEntry entry = DeserializeLogEntry(entryBytes);
            
            if (entry?.Command != null)
            {
                _clusterState.ApplyCommand(entry.Command);
                _logger.LogDebug($"Applied entry {_lastApplied} to state machine");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError($"Failed to apply entry {_lastApplied}: {ex.Message}");
            break; // Stop on error
        }
    }
}


    // =========================
    // SERIALIZATION
    // =========================
        public byte[] SerializeLogEntry(LogEntry logEntry)
    {
        return JsonSerializer.SerializeToUtf8Bytes(logEntry);
    }

    public LogEntry? DeserializeLogEntry(byte[] logEntryBytes)
    {
        if (logEntryBytes == null || logEntryBytes.Length == 0)
            return null;

        if (logEntryBytes.Length == 1 && logEntryBytes[0] == 0xFF)
            return null;

        return JsonSerializer.Deserialize<LogEntry>(logEntryBytes) 
            ?? throw new Exception("Failed to deserialize LogEntry.");
    }



    // =========================
    // LOG TERM HELPERS
    // =========================
    public async Task<int> GetLastTermAsync()
    {
        if (_raftLog is null || _raftLog.CurrentOffset == 0) return 0;

        byte[] lastLogBytes = await _raftLog.ReadAsync(LastLogIndex - 1);

        LogEntry logEntry = DeserializeLogEntry(lastLogBytes);

        if (logEntry is null) return 0;

        return logEntry.Term;
        
    }

    private async Task<int> GetTermByIndex(long index)
    {
        // Raft convention: the term at the very beginning of time is 0
        if (index < 0) return 0;  
    
        if (index >= _raftLog.CurrentOffset) return 0; 

        try 
        {
            byte[] logBytes = await _raftLog.ReadAsync(index);
            LogEntry log = DeserializeLogEntry(logBytes);
            return log?.Term ?? 0;
        }
        catch (Exception)
        {
            // If the log is shorter than we thought, we treat it as term 0
            return 0;
        }
    }


    // =========================
    // CLUSTER STATE QUERIES
    // =========================
    public List<Broker> GetBrokerList()
    {
        return _clusterMembers;
    }

    public async Task<(byte[] data, int count)> GetSerializedTopicMetadataAsync(string topic)
    {
        return await _clusterState.GetSerializedTopicMetadataAsync(topic);
    }

    public async Task<(byte[] data, int count)> GetSerializedBrokersAsync()
    {
        return await _clusterState.GetSerializedBrokersAsync();
    }

    public async Task<int[]> GetListOfPartitionIdsByTopicAndBroker(string topic,int brokerId)
    {
        return await _clusterState.GetListofTopicPartitionsByBrokerId(topic,brokerId);
    }

}
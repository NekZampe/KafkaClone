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
    private readonly ClusterState _clusterState;
    private readonly ILogger<Partition> _logger;
    
    // The current state (starts as Follower)
    public NodeState State { get; private set; } = NodeState.Follower;

    // We need a list of peers to ask for votes
    private List<Broker> _clusterMembers;

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

    private long _lastApplied = 0;
    public long LastApplied => _lastApplied;

    private Dictionary<int,long> _nextIndex; // Used to find common anchor

    private Dictionary<int,long> _matchIndex; // Highest Index of follower commited

    private RaftNode(string basePath,Broker identity, List<Broker> clusterMembers,ILogger<Partition> logger,IRaftTransport raftTransport, Partition raftLog, RaftNodeData nodeData,ClusterState clusterState)
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

    public static async Task<RaftNode> InitializeNode(
        string basePath, 
        Broker identity,
        ILogger<Partition> partitionLogger, 
        IRaftTransport raftTransport,
        ClusterState clusterState)
    {
        var clusterMembers = clusterState.GetAllBrokers();
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
            Partition partition = new Partition(0,newFolder, false, partitionLogger, TimeSpan.FromHours(5), 1024);

            
            return new RaftNode(basePath,identity,clusterMembers,partitionLogger,raftTransport,partition,initialData,clusterState);
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
                    raftNodeState.RaftNodeData,
                    clusterState);

                await raftNode.ReplayLog();

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
                var entriesToSend = new List<LogEntry>();

                // FIX: Actually fetch entries if the follower is behind
                if (LastLogIndex >= nextIdx)
                {
                    entriesToSend = await GetLogEntries(nextIdx, LastLogIndex); 
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
                    Entries = entriesToSend // FIX: Now sends actual data
                };

                var response = await _raftTransport.SendAppendEntriesRequest(heartbeat, broker);
                
                // FIX: Actually handle the response!
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


// Helper to retrieve a range of logs from storage
public async Task<List<LogEntry>> GetLogEntries(long startIndex, long endIndex)
{

    int count = (int) (endIndex - startIndex);
    var list = new List<LogEntry>();

    var result = await _raftLog.ReadBatchAsync(startIndex,count);

    foreach(var logBytes in result.Messages)
    {
        list.Add(DeserializeLogEntry(logBytes));
    }

    return list;
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


public async Task<ForwardCommandResponse> Propose(IClusterCommand command)
{
    // 1. Forwarding Logic
    if (_myIdentity.Id != CurrentLeaderId)
    {
        var leaderBroker = _clusterMembers.FirstOrDefault(broker => broker.Id == CurrentLeaderId);
        if (leaderBroker == null) return new ForwardCommandResponse
        {
            Success = false,
            ErrorMessage = $"No Leader Found"
        };
        var response = await _raftTransport.ForwardCommand(command, leaderBroker); 
        return response;
    }


    // SEND TO CLUSTER 

    // 2. Leader Logic: Create Log Entry
    // Note: The Switch statement usually happens during 'Apply', not 'Propose'. 
    // Here we just want to get it into the log safely.
    var newEntry = new LogEntry
    {
        Term = CurrentTerm,
        Index = LastLogIndex + 1, // Determine new index
        Command = command
    };

    await _raftLog.AppendAsync(SerializeLogEntry(newEntry));
    
    // We match our own index immediately
    _matchIndex[_myIdentity.Id] = LastLogIndex; 
    _nextIndex[_myIdentity.Id] = LastLogIndex + 1;

    // 5. Trigger replication 
    _ = SendHeartbeats(); 

    // We return true that we accepted it. 
    return new ForwardCommandResponse
    {
        Success = true,
        ErrorMessage = null
    }; 
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

    public async Task UpdateLastApplied(long index)
    {
        _lastApplied = index;
    }

    private async Task ReplayLog()
{
    _logger.LogInformation("Replaying Raft Log to restore Cluster State...");
    
     _lastApplied = 0;

    // 1. Iterate through the entire log from the beginning
    long logSize = _raftLog.CurrentOffset; 
    
    for (long index = 0; index < logSize; index++)
    {
        try 
        {
            // 2. Read Raw Bytes
            byte[] entryBytes = await _raftLog.ReadAsync(index);
            
            // 3. Deserialize (We need a wrapper for Term + Command)
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
        }
        catch (Exception ex)
        {
            _logger.LogError($"Failed to replay log at index {index}: {ex.Message}");
            
        }
    }

    // Sync our "Applied" counter to the end of the log
    _logger.LogInformation($"Replay complete. Last Applied Index: {_lastApplied}");
}

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




    // TODO
    // Add GetSerialized Brokers
    // Create GRPC Loop for receiivng cmds
    // ADD client SDK
    // TEST TEST TEST



}
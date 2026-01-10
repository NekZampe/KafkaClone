using KafkaClone.Server.DTOs;

public class AppendEntriesRequest
{
    // 1. Authority
    public int Term { get; set; }
    public int LeaderId { get; set; }

    // 2. Safety
    public long PrevLogIndex { get; set; }
    public int PrevLogTerm { get; set; } 

    // 3. Execution:
    public long LeaderCommit { get; set; }

   // 4. The Payload: The actual log entries to store
    public List<LogEntry> Entries { get; set; } 

    public override string ToString()
    {
        int count = Entries?.Count ?? 0;
        string entriesSummary = count > 0 
            ? $"(First: {Entries[0].Index})" 
            : "(empty)";
            
        return $"[AppendEntries] Leader={LeaderId}, Term={Term}, Prev={PrevLogIndex}(T{PrevLogTerm}), Commit={LeaderCommit}, Entries={count} {entriesSummary}";
    }

}
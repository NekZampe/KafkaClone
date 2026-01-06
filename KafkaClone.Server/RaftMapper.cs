using Google.Protobuf;
using KafkaClone.Server;
using KafkaClone.Server.DTOs;
using KafkaClone.Shared.Grpc;
using System.Text.Json;

public static class RaftMapper
{
    // =========================
    // LogEntry
    // =========================

    public static LogEntryProto ToProto(LogEntry entry)
    {
        byte[] commandBytes = JsonSerializer.SerializeToUtf8Bytes(entry.Command);

        return new LogEntryProto
        {
            Term = entry.Term,
            Index = entry.Index,
            Data = ByteString.CopyFrom(commandBytes)
        };
    }

    public static LogEntry ToInternal(LogEntryProto proto)
    {
        return new LogEntry
        {
            Term = proto.Term,
            Index = proto.Index,
            Command = DeserializeCommand(proto.Data.ToByteArray())
        };
    }

    // =========================
    // Command
    // =========================

    public static CommandProto ToProto(IClusterCommand command)
    {
        byte[] commandBytes = JsonSerializer.SerializeToUtf8Bytes(command);

        return new CommandProto
        {
            Data = ByteString.CopyFrom(commandBytes)
        };
    }

    public static IClusterCommand ToInternal(CommandProto proto)
    {
        return DeserializeCommand(proto.Data.ToByteArray());
    }

    // =========================
    // Command Response
    // =========================

    public static ForwardCommandResponseProto ToProto(ForwardCommandResponse response)
    {

        return new ForwardCommandResponseProto
        {
            Success = response.Success,
            ErrorMessage = response.ErrorMessage
        };
    }

    public static ForwardCommandResponse ToInternal(ForwardCommandResponseProto proto)
    {
        return new ForwardCommandResponse
        {
            Success = proto.Success,
            ErrorMessage = proto.ErrorMessage
        };
    }


    // =========================
    // RequestVote
    // =========================

    public static RequestVoteRequestProto ToProto(RequestVoteRequest request)
    {
        return new RequestVoteRequestProto
        {
            BrokerId = request.BrokerId,
            ElectionTerm = request.ElectionTerm,
            LastLogIndex = request.LastLogIndex,
            LastLogTerm = request.LastLogTerm
        };
    }

    public static RequestVoteRequest ToInternal(RequestVoteRequestProto proto)
    {
        return new RequestVoteRequest
        {
            BrokerId = proto.BrokerId,
            ElectionTerm = proto.ElectionTerm,
            LastLogIndex = proto.LastLogIndex,
            LastLogTerm = proto.LastLogTerm
        };
    }

    public static RequestVoteResponseProto ToProto(RequestVoteResponse response)
    {
        return new RequestVoteResponseProto
        {
            Verdict = response.Verdict,
            Term = response.CurrentTerm
        };
    }

    public static RequestVoteResponse ToInternal(RequestVoteResponseProto proto)
    {
        return new RequestVoteResponse
        {
            Verdict = proto.Verdict,
            CurrentTerm = proto.Term
        };
    }

    // =========================
    // AppendEntries
    // =========================

    public static AppendEntriesRequestProto ToProto(AppendEntriesRequest request)
    {
        var proto = new AppendEntriesRequestProto
        {
            Term = request.Term,
            LeaderId = request.LeaderId,
            PrevLogIndex = request.PrevLogIndex,
            PrevLogTerm = request.PrevLogTerm,
            LeaderCommit = request.LeaderCommit
        };

        proto.Entries.AddRange(request.Entries.Select(ToProto));
        return proto;
    }

    public static AppendEntriesRequest ToInternal(AppendEntriesRequestProto proto)
    {
        return new AppendEntriesRequest
        {
            Term = proto.Term,
            LeaderId = proto.LeaderId,
            PrevLogIndex = proto.PrevLogIndex,
            PrevLogTerm = proto.PrevLogTerm,
            LeaderCommit = proto.LeaderCommit,
            Entries = proto.Entries.Select(ToInternal).ToList()
        };
    }

    public static AppendEntriesResponseProto ToProto(AppendEntriesResponse response)
    {
        return new AppendEntriesResponseProto
        {
            Success = response.Success,
            Term = response.Term,
            LastLogIndex = response.LastLogIndex
        };
    }

    public static AppendEntriesResponse ToInternal(AppendEntriesResponseProto proto)
    {
        return new AppendEntriesResponse
        {
            Success = proto.Success,
            Term = proto.Term,
            LastLogIndex = proto.LastLogIndex
        };
    }

    // =========================
    // Command Deserialization
    // =========================

    private static IClusterCommand DeserializeCommand(byte[] data)
    {
        using var doc = JsonDocument.Parse(data);

        if (!doc.RootElement.TryGetProperty("CommandType", out var typeProp))
            throw new Exception("CommandType missing from log entry");

        string type = typeProp.GetString()!;

        return type switch
        {
            "CreateTopic" => JsonSerializer.Deserialize<CreateTopic>(data)!,
            "RegisterBroker" => JsonSerializer.Deserialize<RegisterBroker>(data)!,
            _ => throw new Exception($"Unknown command type: {type}")
        };
    }
}

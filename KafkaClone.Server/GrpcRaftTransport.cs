using Grpc.Net.Client;
using KafkaClone.Server;
using KafkaClone.Server.DTOs;
using KafkaClone.Shared;
using KafkaClone.Shared.Grpc;


public class GrpcRaftTransport : IRaftTransport
{
    // Holds the active gRPC clients for each broker
    private readonly Dictionary<int, RaftService.RaftServiceClient> _clients = new();
    
    // We keep the channels so we can dispose of them properly later
    private readonly List<GrpcChannel> _channels = new();

    public GrpcRaftTransport(List<Broker> clusterMembers)
    {
        foreach (var member in clusterMembers)
        {
            // FIX: Use GrpcPort instead of Port
            var address = $"http://{member.Host}:{member.GrpcPort}";
            var channel = GrpcChannel.ForAddress(address);
            var client = new RaftService.RaftServiceClient(channel);
            
            _channels.Add(channel);
            _clients[member.Id] = client;
        }
    }

public async Task<RequestVoteResponse> SendRequestVoteRequest(RequestVoteRequest request, Broker broker)
{
    try
    {
        // 1. Get the stub for this specific broker
        var client = _clients[broker.Id];

        // 2. Map internal DTO to Protobuf message
        var protoRequest = RaftMapper.ToProto(request);

        // 3. Perform the gRPC call with a timeout (optional but recommended)
        var protoResponse = await client.RequestVoteAsync(protoRequest);

        // 4. Map back to our internal response type
        return RaftMapper.ToInternal(protoResponse);
    }
    catch (Exception ex)
    {
        // If the broker is offline, we return a response where vote is NOT granted
        // and the term is -1 (or the request term) to signify a failure.
        return new RequestVoteResponse 
        { 
            Verdict = false, 
            CurrentTerm = request.ElectionTerm
        };
    }
}

public async Task<AppendEntriesResponse> SendAppendEntriesRequest(AppendEntriesRequest request, Broker broker)
    {

        try
        {
        // 1. Get the stub for this specific broker
        var client = _clients[broker.Id];

        // 2. Map internal DTO to Protobuf message
        var protoRequest = RaftMapper.ToProto(request);

        // 3. Perform the gRPC call with a timeout (optional but recommended)
        var protoResponse = await client.AppendEntriesAsync(protoRequest);

        // 4. Map back to our internal response type
        return RaftMapper.ToInternal(protoResponse);

            
        } catch (Exception ex)
        {
            return new AppendEntriesResponse
            {
                Success = false,
                Term = request.Term,
                LastLogIndex = request.PrevLogIndex
            };
        }
        
    }


    public async Task<ForwardCommandResponse> ForwardCommand(IClusterCommand request, Broker broker)
    {
        try
        {
            // 1. Get the stub for this specific broker
            var client = _clients[broker.Id];

            // 2. Map internal DTO to Protobuf message
            var protoRequest = RaftMapper.ToProto(request);

            // 3. Perform the gRPC call with a timeout (optional but recommended)
            var protoResponse = await client.ForwardCommandAsync(protoRequest);

            // 4. Map back to our internal response type
            return RaftMapper.ToInternal(protoResponse);
        }
        catch (Exception ex)
        {
            return new ForwardCommandResponse
            {
                Success = false,
                ErrorMessage = $"Failed to forward command: {ex.Message}"
            };
        }
    }
}
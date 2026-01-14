using KafkaClone.Server;
using KafkaClone.Service;
using KafkaClone.Shared.Grpc;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace KafkaClone.Service;

public class RaftGrpcService : RaftService.RaftServiceBase
{
    private readonly BrokerService _brokerService;
    private readonly ILogger<RaftGrpcService> _logger;

    public RaftGrpcService(BrokerService brokerService, ILogger<RaftGrpcService> logger)
    {
        _brokerService = brokerService;
        _logger = logger;
    }

    public override async Task<RequestVoteResponseProto> RequestVote(
        RequestVoteRequestProto request,
        ServerCallContext context)
    {
        var internalRequest = RaftMapper.ToInternal(request);
        var response = await _brokerService.HandleRequestVote(internalRequest);
        return RaftMapper.ToProto(response);
    }

    public override async Task<AppendEntriesResponseProto> AppendEntries(
        AppendEntriesRequestProto request,
        ServerCallContext context)
    {
        var internalRequest = RaftMapper.ToInternal(request);
        var response = await _brokerService.HandleAppendEntries(internalRequest);
        return RaftMapper.ToProto(response);
    }

    public override async Task<ForwardCommandResponseProto> ForwardCommand(
        CommandProto request,
        ServerCallContext context)
    {
        var command = RaftMapper.ToInternal(request);
        var response = await _brokerService.HandleForwardCommand(command);
        return RaftMapper.ToProto(response);
    }
}
using System.Diagnostics.CodeAnalysis;
using Grpc.Core;

namespace Clickhouse.Pure.Grpc;

public record struct CommitBulkReponse
{
    public CommitBulkReponse(
        Result? responseResult,
        RpcException? exception)
    {
        ResponseResult = responseResult;
        Exception = exception;
    }

    public Result? ResponseResult { get; }

    public RpcException? Exception { get; }

    [MemberNotNullWhen(returnValue: true, nameof(Exception))]
    public bool IsFailed()
    {
        return Exception != null;
    }

    [MemberNotNullWhen(returnValue: true, nameof(ResponseResult))]
    public bool IsMetadataResponse()
    {
        return Exception == null
               && ResponseResult != null;
    }
}

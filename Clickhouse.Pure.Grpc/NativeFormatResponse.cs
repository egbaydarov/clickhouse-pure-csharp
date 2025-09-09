using System.Diagnostics.CodeAnalysis;
using Grpc.Core;
using Clickhouse.Pure.ColumnCodeGenerator;

namespace Clickhouse.Pure.Grpc;

public record struct NativeFormatResponse
{
    public NativeFormatResponse(
        Result? responseResult,
        NativeFormatBlockReader? blockReader,
        RpcException? exception,
        bool completed)
    {
        ResponseResult = responseResult;
        BlockReader = blockReader;
        Exception = exception;
        Completed = completed;
    }

    public bool Completed { get; }

    public NativeFormatBlockReader? BlockReader { get; }

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
               && BlockReader == null
               && ResponseResult != null;
    }

    [MemberNotNullWhen(returnValue: true, nameof(BlockReader))]
    public bool IsBlock()
    {
        return Exception == null
               && BlockReader != null;
    }
}

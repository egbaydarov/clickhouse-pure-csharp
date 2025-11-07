using System;
using System.Threading.Tasks;
using Grpc.Core;
using Clickhouse.Pure.Columns;

namespace Clickhouse.Pure.Grpc;

public class NativeBulkReader : IDisposable
{
    private readonly AsyncServerStreamingCall<Result>? _asyncResultReader;
    private readonly RpcException? _initialExceptionOnCreation;

    public NativeBulkReader(
        AsyncServerStreamingCall<Result>? asyncResultReader,
        RpcException? initialExceptionOnCreation)
    {
        _asyncResultReader = asyncResultReader;
        _initialExceptionOnCreation = initialExceptionOnCreation;
    }

    public async Task<NativeFormatResponse> ReadNext()
    {
        if (this._asyncResultReader == null)
        {
            return new NativeFormatResponse(
                responseResult: null,
                exception: _initialExceptionOnCreation,
                blockReader: null,
                completed: true);
        }

        try
        {
            var hasSome = await _asyncResultReader.ResponseStream.MoveNext();
            if (!hasSome)
            {
                return new NativeFormatResponse(
                    responseResult: null,
                    exception: null,
                    blockReader: null,
                    completed: true);
            }

            var cur = this._asyncResultReader.ResponseStream.Current;
            if (cur.Exception != null || cur.Output.IsEmpty)
            {
                return new NativeFormatResponse(
                    responseResult: cur,
                    exception: null,
                    blockReader: null,
                    completed: false);
            }

            var bytes = cur.Output;

            return new NativeFormatResponse(
                responseResult: cur,
                exception: null,
                blockReader: new NativeFormatBlockReader(bytes.Memory),
                completed: false);
        }
        catch (System.Exception ex)
        {
            return new NativeFormatResponse(
                exception: new RpcException(new Status(StatusCode.Unavailable, ex.Message, ex)),
                responseResult: null,
                blockReader: null,
                completed: false);
        }
    }

    public void Dispose()
    {
        _asyncResultReader?.Dispose();
    }
}

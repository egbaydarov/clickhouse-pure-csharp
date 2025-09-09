using System;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;

namespace Clickhouse.Pure.Grpc;

public class BulkWriter : IDisposable
{
    private AsyncClientStreamingCall<QueryInfo, Result>? _asyncResultWriter;
    private RpcException? _asyncException;

    public BulkWriter(
        AsyncClientStreamingCall<QueryInfo, Result>? asyncResultWriter,
        RpcException? asyncException)
    {
        _asyncException = asyncException;
        _asyncResultWriter = asyncResultWriter;
    }

    public async Task<bool> WriteRowsBulkAsync(
        ReadOnlyMemory<byte> inputData,
        bool hasMoreData)
    {
        try
        {
            if (_asyncResultWriter == null)
            {
                return false;
            }

            if (!inputData.IsEmpty)
            {
                await _asyncResultWriter.RequestStream.WriteAsync(
                    message: new QueryInfo
                    {
                        InputData = UnsafeByteOperations.UnsafeWrap(inputData),
                        NextQueryInfo = hasMoreData
                    });
            }

            return true;
        }
        catch (System.Exception ex)
        {
            try
            {
                var exception = await _asyncResultWriter!.ResponseAsync;
                if (exception.Exception != null)
                {
                    _asyncException = new RpcException(
                        status: new Status(
                            statusCode: StatusCode.Unavailable,
                            detail: exception.Exception.ToString(),
                            debugException: ex));

                    return false;
                }
            }
            catch (System.Exception innerException)
            {
                _asyncException = new RpcException(
                    status: new Status(
                        statusCode: StatusCode.Unavailable,
                        detail: innerException.Message,
                        debugException: ex));

                return false;
            }

            _asyncException = new RpcException(
                status: new Status(statusCode: StatusCode.Unavailable, detail: ex.Message, debugException: ex));

            return false;
        }
    }

    public async Task<CommitBulkReponse> Commit()
    {
        try
        {
            if (_asyncResultWriter == null
                || _asyncException != null)
            {
                return new CommitBulkReponse(
                    responseResult: null,
                    exception: _asyncException);
            }

            var response = await _asyncResultWriter.ResponseAsync;

            return new CommitBulkReponse(
                responseResult: response,
                exception: null);
        }
        catch (System.Exception ex)
        {
            return new CommitBulkReponse(
                responseResult: null,
                exception: new RpcException(status: new Status(statusCode: StatusCode.Unavailable,
                    detail: ex.Message, debugException: ex)));
        }
    }

    public void Dispose()
    {
        if (_asyncResultWriter != null)
        {
            _asyncResultWriter.Dispose();
            _asyncResultWriter = null;
        }
    }
}

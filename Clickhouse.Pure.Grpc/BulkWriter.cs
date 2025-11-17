using System;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;

namespace Clickhouse.Pure.Grpc;

public class BulkWriter : IDisposable
{
    private AsyncClientStreamingCall<QueryInfo, Result>? _asyncResultWriter;
    private WriteError? _error;

    private readonly Action<WriteError>? _onException;
    private bool _isCommited;

    public BulkWriter(
        AsyncClientStreamingCall<QueryInfo, Result>? asyncResultWriter,
        WriteError? error,
        Action<WriteError>? onException = null)
    {
        _onException = onException;
        _error = error;
        _asyncResultWriter = asyncResultWriter;
    }

    public async Task<WriteError?> WriteNext(
        ReadOnlyMemory<byte> inputData,
        bool hasMoreData)
    {
        try
        {
            if (_error != null)
            {
                return WriteError(_error);
            }

            if (_isCommited)
            {
                throw new InvalidOperationException("Writer commited. (Only single commit per one writer)");
            }

            await _asyncResultWriter!.RequestStream.WriteAsync(
                message: new QueryInfo
                {
                    InputData = UnsafeByteOperations.UnsafeWrap(inputData),
                    NextQueryInfo = hasMoreData
                });

            if (!hasMoreData)
            {
                await _asyncResultWriter.RequestStream.CompleteAsync();
            }

            return null;
        }
        catch (System.Exception ex)
        {
            try
            {
                var result = await _asyncResultWriter!.ResponseAsync;
                if (result.Exception != null)
                {
                    return WriteError(new WriteError
                    {
                        Exception = null,
                        ClickhouseException = result.Exception,
                    });

                }
            }
            catch (System.Exception innerException)
            {
                return WriteError(new WriteError
                {
                    Exception = innerException,
                    ClickhouseException = null,
                });
            }

            return WriteError(new WriteError
            {
                Exception = ex,
                ClickhouseException = null,
            });
        }
    }

    public async Task<(WriteProgress Progress, WriteError? Error)> Commit()
    {
        try
        {
            if (_error != null)
            {
                return CommitError(_error);
            }
            _isCommited = true;

            var response = await _asyncResultWriter!.ResponseAsync;

            if (response.Exception != null)
            {
                return CommitError(
                    new WriteError()
                    {
                        Exception = null,
                        ClickhouseException = response.Exception,
                    });
            }

            return (
                new WriteProgress()
                {
                    WrittenBytes = (long)response.Progress.WrittenBytes,
                    WrittenRows = (long)response.Progress.WrittenRows,
                }, 
                null);
        }
        catch (System.Exception ex)
        {
            return CommitError(
                new WriteError()
                {
                    Exception = ex,
                    ClickhouseException = null,
                });
        }
    }

    private WriteError WriteError(
        WriteError error)
    {
        _onException?.Invoke(obj: error);
        _error = error;

        return error;
    }

    private (WriteProgress Progress, WriteError? Error) CommitError(
        WriteError error)
    {
        _onException?.Invoke(obj: error);

        return (new WriteProgress
            {
                WrittenBytes = 0,
                WrittenRows = 0
            },
            error);
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
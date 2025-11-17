using System;
using System.Threading.Tasks;
using Grpc.Core;
using Clickhouse.Pure.Columns;

namespace Clickhouse.Pure.Grpc;

public sealed class BulkReader : IDisposable
{
    private AsyncServerStreamingCall<Result>? _asyncResultReader;
    private ReadError? _error;

    private readonly Action<ReadProgress>? _onProgress;
    private readonly Action<ReadError>? _onException;

    private readonly ReadProgress _readTotalProgress = new()
    {
        ReadRows = 0,
        ReadBytes = 0,
        TotalRowsToRead = 0
    };

    public BulkReader(
        AsyncServerStreamingCall<Result>? asyncResultReader,
        ReadError? error,
        Action<ReadError>? onException = null,
        Action<ReadProgress>? onProgress = null)
    {
        _onException = onException;
        _onProgress = onProgress;
        _asyncResultReader = asyncResultReader;
        _error = error;
    }

    public async Task<NativeFormatBlockReader?> Read()
    {
        if (_error != null)
        {
            return SetError(
                error: _error);
        }

        try
        {
            var hasSome = await _asyncResultReader!.ResponseStream.MoveNext();
            if (!hasSome)
            {
                return null;
            }

            var cur = this._asyncResultReader.ResponseStream.Current;
            if (cur.Exception != null)
            {
                return SetError(
                    error: new ReadError()
                    {
                        Exception = null,
                        ClickhouseException = cur.Exception,
                    });
            }
            if (cur.Progress != null)
            {
                // between blocks (if many) clickhouse reports progress
                _onProgress?.Invoke(
                    obj: new ReadProgress()
                    {
                        ReadBytes = (long)cur.Progress.ReadBytes,
                        ReadRows = (long)cur.Progress.ReadRows,
                        TotalRowsToRead = (long)cur.Progress.TotalRowsToRead,
                    });
                _readTotalProgress.TotalRowsToRead = (long)cur.Progress.TotalRowsToRead;
                _readTotalProgress.ReadBytes += (long)cur.Progress.ReadBytes;
                _readTotalProgress.ReadRows += (long)cur.Progress.ReadRows;

                return await Read();
            }

            var bytes = cur.Output;
            var block = new NativeFormatBlockReader(bytes: bytes.Memory);

            return block;
        }
        catch (System.Exception ex)
        {
            return SetError(
                error: new ReadError
                {
                    Exception = ex,
                    ClickhouseException = null,
                });
        }
    }

    public (ReadProgress LastProgress, ReadError? Error) GetState()
    {
        return (_readTotalProgress, _error);
    }

    private NativeFormatBlockReader? SetError(
        ReadError error)
    {
        _onException?.Invoke(obj: error);
        _error = error;

        return null;
    }

    public void Dispose()
    {
        if (_asyncResultReader != null)
        {
            _asyncResultReader.Dispose();
            _asyncResultReader = null;
        }
    }
}

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
            return SetError(_error);
        }

        try
        {
            // Keep reading until the stream is exhausted
            while (await _asyncResultReader!.ResponseStream.MoveNext())
            {
                var cur = _asyncResultReader.ResponseStream.Current;

                // 1. Progress handling (and maybe output)
                if (cur.Progress != null)
                {
                    var progress = cur.Progress;

                    var readProgress = new ReadProgress
                    {
                        ReadBytes       = (long)progress.ReadBytes,
                        ReadRows        = (long)progress.ReadRows,
                        TotalRowsToRead = (long)progress.TotalRowsToRead
                    };

                    _onProgress?.Invoke(readProgress);

                    _readTotalProgress.TotalRowsToRead = (long)progress.TotalRowsToRead;
                    _readTotalProgress.ReadBytes      += (long)progress.ReadBytes;
                    _readTotalProgress.ReadRows       += (long)progress.ReadRows;

                    if (cur.Output != null && !cur.Output.Memory.IsEmpty)
                    {
                        return new NativeFormatBlockReader(cur.Output.Memory);
                    }
                }

                // 2. Stats handling (and maybe output)
                if (cur.Stats != null)
                {
                    // TODO: handle
                    if (cur.Output != null && !cur.Output.Memory.IsEmpty)
                    {
                        return new NativeFormatBlockReader(cur.Output.Memory);
                    }
                }

                // 3. Exception handling
                if (cur.Exception != null)
                {
                    return SetError(new ReadError
                    {
                        Exception         = null,
                        ClickhouseException = cur.Exception
                    });
                }

                // 4. Plain output (no progress/stats)
                if (cur.Output != null && !cur.Output.Memory.IsEmpty)
                {
                    return new NativeFormatBlockReader(cur.Output.Memory);
                }

                // the loop continues to read the next one
            }

            // Stream ended
            return null;
        }
        catch (System.Exception ex)
        {
            return SetError(new ReadError
            {
                Exception = ex,
                ClickhouseException = null
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
        _asyncResultReader?.Dispose();
        _asyncResultReader = null;
    }
}

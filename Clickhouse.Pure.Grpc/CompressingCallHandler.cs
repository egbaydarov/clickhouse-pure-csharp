using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;

namespace Clickhouse.Pure.Grpc;

public sealed class CompressingCallHandler : IDisposable
{
    private readonly ClickHouseGrpcRouter _router;
    private readonly QueryInfo _baseQueryInfo;

    private readonly TimeSpan? _queryTimeout;
    private readonly MapField<string, string> _defaultSettings;

    public CompressingCallHandler(
        ClickHouseGrpcRouter router,
        string password,
        string username,
        IDictionary<string, string>? defaultSettings = null,
        TimeSpan? queryTimeout = null,
        int compressionLevel = 3,
        string compression = "gzip")
    {
        _baseQueryInfo = new QueryInfo()
        {
            UserName = username,
            Password = password,
            TransportCompressionLevel = compressionLevel,
            TransportCompressionType = compression,
        };
        _router = router;

        var settings = new MapField<string, string>();
        if (defaultSettings != null)
        {
            settings.MergeFrom(defaultSettings);
        }

        _defaultSettings = settings;
        _queryTimeout = queryTimeout;
    }

    /// <summary>
    /// Initiate session (error details will be wrapped by writer)
    /// </summary>
    /// <param name="initialQuery">Use constant for efficiency and avoiding injections</param>
    /// <param name="database">Default in query contains only table name</param>
    /// <param name="inputDataDelimiter">
    /// Use that param for formats like CSV,
    /// TabSeparated, TSKV, JSONEachRow, Template, CustomSeparated and Protobuf</param>
    /// <param name="settings">arbitrary settings supported by clickhouse server (merged with default provided in constructor)</param>
    /// <returns></returns>
    public async Task<BulkWriter> InputBulk(
        string initialQuery,
        string? database = null,
        string inputDataDelimiter = "",
        Dictionary<string,string>? settings = null)
    {
        var call = _router.Call<AsyncClientStreamingCall<QueryInfo, Result>>(
            handler: async (client, ct) =>
            {
                var result = client
                    .ExecuteQueryWithStreamInput(
                        deadline: GetQueryDeadline(),
                        cancellationToken: ct);

                //TODO: tests
                var querySettings = _defaultSettings.Clone();
                if (settings != null)
                {
                    querySettings.MergeFrom(settings);
                }

                var sessionId = Guid.NewGuid().ToString();
                var queryId = Guid.NewGuid().ToString();
                await result.RequestStream.WriteAsync(
                    message: new QueryInfo
                    {
                        UserName = _baseQueryInfo.UserName,
                        Password = _baseQueryInfo.Password,
                        TransportCompressionLevel = _baseQueryInfo.TransportCompressionLevel,
                        TransportCompressionType = _baseQueryInfo.TransportCompressionType,
                        Query = initialQuery,
                        Settings = { querySettings },
                        NextQueryInfo = true,
                        Database = database ?? _baseQueryInfo.Database,
                        QueryId = queryId,
                        SessionId = sessionId,
                        InputDataDelimiter = UnsafeByteOperations.UnsafeWrap(
                            bytes: Encoding.UTF8.GetBytes(s: inputDataDelimiter)
                                .AsMemory()),
                    }, cancellationToken: ct);

                return result;
            },
            logHandler: Console.WriteLine);

        try
        {
            var clientStreamingCall = await call;

            return new BulkWriter(
                asyncResultWriter: clientStreamingCall,
                error: null);
        }
        catch (System.Exception ex)
        {
            return new BulkWriter(
                asyncResultWriter: null,
                error: new WriteError()
                {
                    ClickhouseException = null,
                    Exception = ex
                });
        }
    }

    public async Task<(string?, RpcException?)> QueryRawString(
        string query,
        string? database = null,
        Dictionary<string,string>? settings = null)
    {
        var call = _router.Call<Result>(
            handler: (client, ct) =>
            {
                var querySettings = _defaultSettings.Clone();
                if (settings != null)
                {
                    querySettings.MergeFrom(settings);
                }
                var queryInfo = new QueryInfo
                {
                    UserName = _baseQueryInfo.UserName,
                    Password = _baseQueryInfo.Password,
                    TransportCompressionLevel = _baseQueryInfo.TransportCompressionLevel,
                    TransportCompressionType = _baseQueryInfo.TransportCompressionType,
                    Query = query,
                    Database = database ?? _baseQueryInfo.Database,
                    Settings = { querySettings }
                };

                var result = client
                    .ExecuteQuery(
                        request: queryInfo,
                        cancellationToken: ct,
                        deadline: GetQueryDeadline());

                return Task.FromResult(result);
            },
            logHandler: Console.WriteLine);

        try
        {
            var result = await call;

            if (result.Exception != null)
            {
                return (null, new RpcException(new Status(
                    statusCode: StatusCode.Internal,
                    detail: result.ToString())));
            }

            if (result.Output.IsEmpty)
            {
                return (result.ToString(), null);
            }

            return (Encoding.UTF8.GetString(result.Output.Span), null);
        }
        catch (System.Exception ex)
        {
            return (null, new RpcException(new Status(
                    statusCode: StatusCode.Unavailable,
                    detail: "Exception on query initiation",
                    debugException: ex)));
        }
    }

    public async Task<(Result?, RpcException?)> QueryRawResult(
        string query,
        string? database = null,
        Dictionary<string,string>? settings = null)
    {
        var call = _router.Call<Result>(
            handler: (client, ct) =>
            {
                var querySettings = _defaultSettings.Clone();
                if (settings != null)
                {
                    querySettings.MergeFrom(settings);
                }

                var queryInfo = new QueryInfo
                {
                    UserName = _baseQueryInfo.UserName,
                    Password = _baseQueryInfo.Password,
                    TransportCompressionLevel = _baseQueryInfo.TransportCompressionLevel,
                    TransportCompressionType = _baseQueryInfo.TransportCompressionType,
                    Query = query,
                    Settings = { querySettings },
                    Database = database ?? _baseQueryInfo.Database,
                };

                var result = client
                    .ExecuteQuery(
                        request: queryInfo,
                        deadline: GetQueryDeadline(),
                        cancellationToken: ct);

                return Task.FromResult(result: result);
            },
            logHandler: Console.WriteLine);

        try
        {
            var result = await call;

            return (result, null);
        }
        catch (System.Exception ex)
        {
            return (null, new RpcException(status: new Status(
                statusCode: StatusCode.Unavailable,
                detail: "Exception on query initiation",
                debugException: ex)));
        }
    }

    public async Task<BulkReader> QueryNativeBulk(
        string query,
        string? database = null,
        Dictionary<string,string>? settings = null)
    {
        var call = _router.Call<AsyncServerStreamingCall<Result>>(
            handler: (client, _) =>
            {
                var querySettings = _defaultSettings.Clone();
                if (settings != null)
                {
                    querySettings.MergeFrom(settings);
                }

                var queryInfo = new QueryInfo
                {
                    UserName = _baseQueryInfo.UserName,
                    Password = _baseQueryInfo.Password,
                    TransportCompressionLevel = _baseQueryInfo.TransportCompressionLevel,
                    TransportCompressionType = _baseQueryInfo.TransportCompressionType,
                    OutputFormat = "Native",
                    Settings = { querySettings },
                    Query = query,
                    Database = database ?? _baseQueryInfo.Database,
                };

                var result = client
                    .ExecuteQueryWithStreamOutput(
                        deadline: GetQueryDeadline(),
                        request: queryInfo);

                return Task.FromResult(result);
            },
            logHandler: Console.WriteLine);

        try
        {
            var asyncResultReader = await call;

            return new BulkReader(
                asyncResultReader: asyncResultReader,
                error: null);

        }
        catch (System.Exception ex)
        {
            return new BulkReader(
                asyncResultReader: null,
                error: new ReadError()
                {
                    ClickhouseException = null,
                    Exception = ex,
                });
        }
    }

    private DateTime? GetQueryDeadline()
    {
        return _queryTimeout == null ? null : DateTime.UtcNow.Add(_queryTimeout.Value);
    }

    public void Dispose()
    {
        _router.Dispose();
    }
}

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;

namespace Clickhouse.Pure.Grpc;

public sealed class DefaultCallHandler : IDisposable
{
    private readonly ClickHouseGrpcRouter _router;
    private readonly QueryInfo _baseQueryInfo;

    private readonly TimeSpan? _queryTimeout;
    private readonly MapField<string, string> _defaultSettings;
    
    public DefaultCallHandler(
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
            TransportCompressionType = compression
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

    public async Task<BulkWriter> InputBulk(
        string initialQuery,
        string delimiter = "",
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
                        SessionId = Guid.NewGuid().ToString(),
                        InputDataDelimiter = UnsafeByteOperations.UnsafeWrap(
                            bytes: Encoding.UTF8.GetBytes(s: delimiter).AsMemory()),
                    }, cancellationToken: ct);
                
                return result;
            },
            logHandler: Console.WriteLine);

        try
        {
            var clientStreamingCall = await call;

            return new BulkWriter(asyncResultWriter: clientStreamingCall, asyncException: null);
        }
        catch (System.Exception ex)
        {
            return new BulkWriter(
                asyncResultWriter: null,
                asyncException: new RpcException(status: new Status(
                    statusCode: StatusCode.Unavailable,
                    detail: ex.Message,
                    debugException: ex)));
        }
    }

    public async Task<(string?, RpcException?)> QueryRawString(
        string query,
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
                    Settings = { querySettings }
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

    public async Task<NativeBulkReader> QueryNativeBulk(
        string query,
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

            return new NativeBulkReader(
                asyncResultReader: asyncResultReader,
                initialExceptionOnCreation: null);

        }
        catch (System.Exception ex)
        {
            return new NativeBulkReader(
                asyncResultReader: null,
                initialExceptionOnCreation: new RpcException(new Status(
                    statusCode: StatusCode.Unavailable,
                    detail: ex.Message,
                    debugException: ex)));
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
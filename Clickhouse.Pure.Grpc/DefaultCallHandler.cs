using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Security;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;

namespace Clickhouse.Pure.Grpc;

public sealed class DefaultCallHandler
{
    private readonly ClickHouseGrpcRouter _router;
    private readonly QueryInfo _baseQueryInfo;
    
    public static readonly ReadOnlyMemory<byte> InputDataDelimiter = new byte[]{ 0x1d }; // ascii group separator 

    public DefaultCallHandler(
        ClickHouseGrpcRouter router,
        SecureString password,
        SecureString username,
        int compressionLevel = 3,
        string compression = "gzip")
    {
        _baseQueryInfo = new QueryInfo()
        {
            UserName = Marshal.PtrToStringUni(Marshal.SecureStringToGlobalAllocUnicode(username)),
            Password = Marshal.PtrToStringUni(Marshal.SecureStringToGlobalAllocUnicode(password)),
            TransportCompressionLevel = compressionLevel,
            TransportCompressionType = compression
        };
        _router = router;
    }

    public async Task<BulkWriter> InputBulk(
        string initialQuery,
        string delimiter = "")
    {
        var call = _router.Call<AsyncClientStreamingCall<QueryInfo, Result>>(
            handler: async (client, ct) =>
            {
                var result = client
                    .ExecuteQueryWithStreamInput(
                        cancellationToken: ct);

                await result.RequestStream.WriteAsync(
                    message: new QueryInfo
                    {
                        UserName = _baseQueryInfo.UserName,
                        Password = _baseQueryInfo.Password,
                        Query = initialQuery,
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
        string query)
    {
        var call = _router.Call<Result>(
            handler: (client, ct) =>
            {
                var queryInfo = new QueryInfo
                {
                    UserName = _baseQueryInfo.UserName,
                    Password = _baseQueryInfo.Password,
                    Query = query,
                };

                var result = client
                    .ExecuteQuery(queryInfo, cancellationToken: ct);

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
        string query)
    {
        var call = _router.Call<Result>(
            handler: (client, ct) =>
            {
                var queryInfo = new QueryInfo
                {
                    UserName = _baseQueryInfo.UserName,
                    Password = _baseQueryInfo.Password,
                    Query = query,
                };

                var result = client
                    .ExecuteQuery(queryInfo, cancellationToken: ct);

                return Task.FromResult(result);
            },
            logHandler: Console.WriteLine);

        try
        {
            var result = await call;
            
            return (result, null);
        }
        catch (System.Exception ex)
        {
            return (null, new RpcException(new Status(
                statusCode: StatusCode.Unavailable,
                detail: "Exception on query initiation",
                debugException: ex)));
        }
    }

    public async Task<NativeBulkReader> QueryNativeBulk(
        [ConstantExpected]
        string query)
    {
        var call = _router.Call<AsyncServerStreamingCall<Result>>(
            handler: (client, _) =>
            {
                var queryInfo = new QueryInfo
                {
                    UserName = _baseQueryInfo.UserName,
                    Password = _baseQueryInfo.Password,
                    OutputFormat = "Native",
                    Query = query,
                    TransportCompressionLevel = 3,
                    TransportCompressionType = "gzip",
                };

                var result = client
                    .ExecuteQueryWithStreamOutput(
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
}
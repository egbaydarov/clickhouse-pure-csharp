using System;
using Grpc.Core;
using Grpc.Net.Client;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Compression;

namespace Clickhouse.Pure.Grpc;

public sealed class ClickHouseGrpcRouter : IDisposable
{
    private readonly int _grpcPort;
    private readonly bool _useSsl;
    private readonly Lock _sync = new();

    private readonly List<string> _seedEndpoints;
    private volatile List<string> _endpoints;

    private FrozenDictionary<string, RoundRobinChannelPool> _channels;

    private readonly GrpcChannelOptions _channelOptions;
    public static readonly GrpcChannelOptions ChannelOptions = new()
    {
        MaxReceiveMessageSize = null, 
        HttpHandler = new SocketsHttpHandler
        {
            KeepAlivePingDelay = TimeSpan.FromSeconds(5),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(20),
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(10),
            EnableMultipleHttp2Connections = true,
        },
        CompressionProviders = [
            new GzipCompressionProvider(CompressionLevel.SmallestSize)
        ]
    };

    private int _primary;

    private sealed class RoundRobinChannelPool : IDisposable
    {
        private readonly int _size;
        private readonly Lock _sync = new();

        private volatile int _next;
        private bool _disposed;
        private readonly string _id = Guid.NewGuid().ToString();

        internal GrpcChannel GetNext()
        {
            lock (_sync)
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(RoundRobinChannelPool));
                }

                var index = _next;
                _next = index + 1;
                var slot = (int)((uint)index % (uint)_size);

                return _pool[slot].Value;
            }
        }

        private readonly List<Lazy<GrpcChannel>> _pool;

        internal RoundRobinChannelPool(
            int size,
            Func<GrpcChannel> factory)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(size);
            ArgumentNullException.ThrowIfNull(factory);

            _pool = new List<Lazy<GrpcChannel>>(size);
            for (var i = 0; i < size; ++i)
            {
                _pool.Add(new Lazy<GrpcChannel>(factory));
            }

            _size = size;
            _next = 0;
            _disposed = false;
        }

        public void Dispose()
        {
            lock (_sync)
            {
                if (_disposed)
                {
                    return;
                }

                foreach (var pool in _pool.Where(pool => pool.IsValueCreated))
                {
                    pool.Value.Dispose();
                }

                _disposed = true;
            }
        }
    }

    public ClickHouseGrpcRouter(
        IEnumerable<string> seedEndpoints,
        ushort port = 9100,
        string username = "default",
        string password = "default",
        int poolSize = 1,
        bool useSsl = false,
        TimeSpan? connectionTimeout = null,
        GrpcChannelOptions? channelOptions = null)
    {
        _seedEndpoints = seedEndpoints.Distinct().ToList();
        if (_seedEndpoints.Count == 0)
        {
            throw new ArgumentException("Need at least one endpoint", nameof(seedEndpoints));
        }

        _endpoints = [.. _seedEndpoints];
        _grpcPort = port;
        _useSsl = useSsl;

        _channelOptions = channelOptions ?? ChannelOptions;
        _channels = FrozenDictionary<string, RoundRobinChannelPool>.Empty;

        InitClusterConnectionPool(
            username: username,
            password: password,
            poolSize: poolSize,
            connectionTimeout: connectionTimeout ?? TimeSpan.FromSeconds(10));
    }

    public async Task<T> Call<T>(
        Func<ClickHouse.ClickHouseClient, CancellationToken, Task<T>> handler,
        Action<string>? logHandler = null,
        int maxTries = 2,
        CancellationToken ct = default)
    {
        var result = Task.FromException<T>(
            new RpcException(new Status(StatusCode.Unavailable, "Not cluster endpoints data found.")));

        for (var i = 0; i < _endpoints.Count; i++)
        {
            var ep = _endpoints[(_primary + i) % _endpoints.Count];
            var channel = _channels[ep].GetNext();

            var client = new ClickHouse.ClickHouseClient(channel);

            try
            {
                return await handler(client, ct);
            }
            catch (System.Exception ex) when (IsRetryable(ex) && i < maxTries)
            {
                if (logHandler != null)
                {
                    logHandler(ex.Message);
                    logHandler($"Retry attempt on channel: {ep}");
                }
                else
                {
                    await Console.Error.WriteAsync(ex.Message);
                    await Console.Error.WriteAsync($"Retry attempt on channel: {ep}");
                }

                // some backoff
                Thread.Sleep(200);
            }
            catch (System.Exception ex)
            {
                var unavailable = new Status(StatusCode.Unavailable, ex.Message, ex);

                result = Task.FromException<T>(new RpcException(unavailable));
            }
        }

        return await result;
    }

    private void InitClusterConnectionPool(string username,
        string password,
        int poolSize,
        TimeSpan connectionTimeout)
    {
        var seedEndpoint = _seedEndpoints[Random.Shared.Next(_seedEndpoints.Count)];
        var client  = new ClickHouse.ClickHouseClient(GrpcChannel.ForAddress(seedEndpoint));

        var discovered = DiscoverClickHouseClusterEndpoints(
            client: client,
            connectionTimeout: connectionTimeout,
            username: username,
            password: password);

        // default advertised hostname for local setup
        // TODO: find better approach
        if (
            discovered is ["http://::1:9100"] ||
            discovered.Contains("http://127.0.0.1:9100"))
        {
            discovered = ["http://127.0.0.1:9100"];
        }

        if (discovered.Count > 0)
        {
            lock (_sync)
            {
                _endpoints = discovered;
                _channels = _endpoints
                    .ToFrozenDictionary(
                        elementSelector: endpoint => new RoundRobinChannelPool(
                            size: poolSize,
                            factory: () => GrpcChannel.ForAddress(
                                address: endpoint,
                                channelOptions: _channelOptions)),
                        keySelector: endpoint => endpoint);
                _primary = Random.Shared.Next(_endpoints.Count);
            }
        }
    }

    private static bool IsRetryable(System.Exception ex)
    {
        if (ex is RpcException rex)
        {
            return rex.StatusCode
                is StatusCode.Unavailable
                or StatusCode.DeadlineExceeded 
                or StatusCode.ResourceExhausted
                or StatusCode.Internal 
                or StatusCode.Aborted;
        }

        return ex is HttpRequestException or TaskCanceledException or TimeoutException;
    }

    private List<string> DiscoverClickHouseClusterEndpoints(
        ClickHouse.ClickHouseClient client,
        TimeSpan connectionTimeout,
        string username,
        string password)
    {
        var result =  client.ExecuteQuery(
            new QueryInfo
            {
                UserName = username,
                Password = password,
                Query = "SELECT host_address FROM system.clusters FORMAT CSV",
            },
            new CallOptions(
                headers: Metadata.Empty,
                deadline: DateTime.UtcNow.Add(connectionTimeout)));

        if (result.Exception != null)
        {
            Console.WriteLine(result.Exception);
            return _endpoints;
        }

        var responseBytes = Encoding.UTF8.GetString(result.Output.Span);
        var eachRowCsv = responseBytes.Split('\n');

        return eachRowCsv
            .Select(row => row.Trim('"'))
            .Where(row => !string.IsNullOrWhiteSpace(row))
            .Distinct()
            .Select(host => $"http{(_useSsl ? "s" : "")}://{host}:{_grpcPort}")
            .ToList();
    }

    public void Dispose()
    {
        foreach (var kv in _channels)
        {
            try { kv.Value.Dispose(); } catch { /* ignore */ }
        }

        _channelOptions.HttpHandler?.Dispose();
    }
}

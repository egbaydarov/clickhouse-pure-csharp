using System;
using System.Collections.Generic;
using Grpc.Net.Client;

namespace Clickhouse.Pure.Grpc;

public static class ClickhouseGrpcConnectionFactory
{
    public static ClickHouseGrpcRouter Create(
        string endpoint,
        string bootstrapUsername,
        string bootstrapPassword,
        ushort port = 9100,
        bool useSsl = true)
    {
        return new ClickHouseGrpcRouter(
            seedEndpoints: [endpoint],
            username: bootstrapUsername,
            password: bootstrapPassword,
            port: port,
            useSsl: useSsl);
    }

    public static ClickHouseGrpcRouter Create(
        IEnumerable<string> endpoints,
        string bootstrapUsername,
        string bootstrapPassword,
        ushort port = 9100,
        bool useSsl = true,
        int poolSize = 1,
        GrpcChannelOptions? options = null,
        TimeSpan? initialBootstrapConnectionTimeout = null)
    {
        return new ClickHouseGrpcRouter(
            seedEndpoints: endpoints,
            username: bootstrapUsername,
            password: bootstrapPassword,
            port: port,
            poolSize: poolSize,
            connectionTimeout: initialBootstrapConnectionTimeout ?? TimeSpan.FromSeconds(5),
            channelOptions: options ?? ClickHouseGrpcRouter.ChannelOptions,
            useSsl: useSsl);
    }
}

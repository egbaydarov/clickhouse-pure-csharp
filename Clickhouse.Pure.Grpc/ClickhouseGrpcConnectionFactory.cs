using System;
using System.Collections.Generic;
using Grpc.Net.Client;

namespace Clickhouse.Pure.Grpc;

public static class ClickhouseGrpcConnectionFactory
{
    public static ClickHouseGrpcRouter CreateNoSniff(
        IEnumerable<Uri> hosts,
        ushort port = 9100,
        int poolSize = 1,
        GrpcChannelOptions? options = null)
    {
        return new ClickHouseGrpcRouter(
            hosts: hosts,
            port: port,
            poolSize: poolSize,
            channelOptions: options);
    }

    public static ClickHouseGrpcRouter Create(
        string endpoint,
        string bootstrapUsername,
        string bootstrapPassword,
        ushort port = 9100,
        bool useSsl = true)
    {
        return new ClickHouseGrpcRouter(
            seedEndpoints: [endpoint],
            port: port,
            username: bootstrapUsername,
            password: bootstrapPassword,
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
            port: port,
            username: bootstrapUsername,
            password: bootstrapPassword,
            poolSize: poolSize,
            useSsl: useSsl,
            connectionTimeout: initialBootstrapConnectionTimeout ?? TimeSpan.FromSeconds(5),
            channelOptions: options ?? ClickHouseGrpcRouter.ChannelOptions);
    }
}

namespace Clickhouse.Pure.Grpc;

public static class ClickhouseGrpcConnectionFactory
{
    public static ClickHouseGrpcRouter Create(
        string endpoint,
        string username,
        string password,
        ushort port)
    {
        return new ClickHouseGrpcRouter(
            seedEndpoints: [endpoint],
            username: username,
            password: password,
            port: port,
            useSsl: false);
    }
}

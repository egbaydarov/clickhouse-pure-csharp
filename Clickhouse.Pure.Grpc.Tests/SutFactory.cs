namespace Clickhouse.Pure.Grpc.Tests;

public static class SutFactory
{
    public static Sut Create()
    {
        var username =  Environment.GetEnvironmentVariable("CLICKHOUSE_USER") ?? "default";
        var password =  Environment.GetEnvironmentVariable("CLICKHOUSE_PASSWORD") ?? "default";
        var router = ClickhouseGrpcConnectionFactory.Create(
            endpoint: "http://127.0.0.1:9100",
            bootstrapUsername: username,
            bootstrapPassword: password,
            useSsl: false);

        var handler = new DefaultCallHandler(
            compression: "gzip",
            router: router, 
            password: password,
            queryTimeout: TimeSpan.FromSeconds(300),
            defaultSettings: new Dictionary<string, string> { { "insert_quorum","auto" } },
            username: username);

        var sut = new Sut(handler);

        return sut;
    }
}
namespace Clickhouse.Pure.Grpc;

public sealed class WriteProgress
{
    public required long WrittenRows { get; init; }
    public required long WrittenBytes { get; init; }
}
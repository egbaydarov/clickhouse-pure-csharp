namespace Clickhouse.Pure.Grpc;

public sealed class ReadProgress
{
    public required long ReadRows { get; set; }
    public required long ReadBytes { get; set; }
    public required long TotalRowsToRead { get; set; }
}

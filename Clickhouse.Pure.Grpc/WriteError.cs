using System;

namespace Clickhouse.Pure.Grpc;

public sealed class WriteError
{
    public required System.Exception? Exception { get; init; }
    public required Exception? ClickhouseException { get; init; }

    public string Message => Exception?.Message ??
                             ClickhouseException?.DisplayText ??
        throw new InvalidOperationException("assert failed: Empty Error result");
}
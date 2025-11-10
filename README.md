<h1 align="center">ClickHouse Pure C# Client</h1>

Unofficial, simple and efficient driver for Clickhouse.
Less memory allocations, native data format, column oriented API.

<br/>

<p align="center">
<a href="https://www.nuget.org/packages/Clickhouse.Pure.Grpc">
<img alt="NuGet Version" src="https://img.shields.io/nuget/v/Clickhouse.Pure.Grpc">
</a>

<a href="https://www.nuget.org/packages/Clickhouse.Pure.Grpc">
<img alt="NuGet Downloads" src="https://img.shields.io/nuget/dt/Clickhouse.Pure.Grpc">
</a>

<a href="https://github.com/egbaydarov/clickhouse-pure-csharp/actions/workflows/tests.yaml">
<img src="https://github.com/egbaydarov/clickhouse-pure-csharp/actions/workflows/tests.yaml/badge.svg?branch=main">
</a>

<a href="https://codecov.io/gh/egbaydarov/clickhouse-pure-csharp" >
<img src="https://codecov.io/gh/egbaydarov/clickhouse-pure-csharp/graph/badge.svg?token=VS1YDF9ICO"/>
</a>

## Why this exists

In my experience, [ClickHouse.Client](https://github.com/DarkWanderer/ClickHouse.Client) and its [successor](https://github.com/ClickHouse/clickhouse-cs) still carry too much legacy .NET slop. ORM? ADO? These abstractions don’t add much value for ClickHouse in 2025.

Their APIs make efficient data ingestion less obvious, and I’ve seen unnecessary heap churn and overly defensive code trying to support every platform.

I personally prefer [clickhouse-go](https://github.com/ClickHouse/clickhouse-go) - it feels simpler, faster, and more aligned with ClickHouse itself.

This project aims to be intentionally minimal yet powerful for modern .NET apps.
That’s just my take, though - if the existing drivers work well for you, use what fits best.

## Under the hood

- Uses gRPC and Protobuf to pack and ship data to the ClickHouse server. (The .NET gRPC client feels much thinner than plain HTTP, and the protocol design still gives the same wins.)
- More column-oriented API that lets you build column arrays in a buffer and send them to the server with as few copies as possible. (Not perfect yet, but it already outperforms the official ClickHouse driver.)
- ArrayPool to cut allocations (plays nicely with the internal allocation strategy in the gRPC .NET clients).
- Ready for .NET AOT with zero warnings.

> **Warning:** For anyone who accidentally lands on this driver: parts of this codebase were vibecoded (written fast and loose). There are probably bugs. Don't blindly deploy this to production without thorough testing in your own environment. Run your own tests, benchmark your workloads, and make sure it actually works for your use case first. The API may also change significantly in future versions

## Performance

This driver is *fast*. Like, really fast compared to the official one. (From x3 to x10 depending on data type and row count.)

Check out more detailed benchmark results in the [Single Column Table](https://github.com/egbaydarov/clickhouse-pure-csharp/blob/main/benchmark-artifacts/Clickhouse.Pure.BenchmarkDotnet.SingleColumnInsertBenchmarks-report-github.md) and [Complex Table](https://github.com/egbaydarov/clickhouse-pure-csharp/blob/main/benchmark-artifacts/Clickhouse.Pure.BenchmarkDotnet.InsertBenchmarks-report-github.md).

## Stuff to Make Better

- General refactoring (trim vibecode artifacts and make the API more straightforward).
- Generate tests from templates instead of hand-rolling them.
- Use bare TCP instead of gRPC (gRPC is temporary to save time), which should give a boost and let manage memory with arenas.
- More type support (right now only what I personally need is implemented).
- More hot-path optimizations and profiling.

## Supported Types

The full mapping table lives in [TYPES.md](./TYPES.md).

## Usage Examples

All of the code samples is here [USAGE.md](./USAGE.md).

## ClickHouse Versions

This driver should work with any ClickHouse version that's currently receiving updates. 

Check [ClickHouse's page](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md#security-change-log-and-support) for the list of supported versions.

## Credits

Big thanks to the [ClickHouse Go driver](https://github.com/ClickHouse/clickhouse-go) team - learned a lot from their native protocol implementation.

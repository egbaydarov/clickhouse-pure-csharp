<h1 align="center">ClickHouse Pure C# Client</h1>

<br/>

<p align="center">
<a href="https://www.nuget.org/packages/Clickhouse.Pure.Grpc">
<img alt="NuGet Version" src="https://img.shields.io/nuget/v/Clickhouse.Pure.Grpc">
</a>

<a href="https://www.nuget.org/packages/Clickhouse.Pure.Grpc">
<img alt="NuGet Downloads" src="https://img.shields.io/nuget/dt/Clickhouse.Pure.Grpc">
</a>

<a href="https://github.com/egbaydarov/clickhouse-pure-csharp/actions/workflows/integration-tests.yaml">
<img src="https://github.com/egbaydarov/clickhouse-pure-csharp/actions/workflows/integration-tests.yaml/badge.svg?branch=main">
</a>

<a href="https://codecov.io/gh/egbaydarov/clickhouse-pure-csharp" >
<img src="https://codecov.io/gh/egbaydarov/clickhouse-pure-csharp/graph/badge.svg?token=VS1YDF9ICO"/>
</a>

## Why this exists

Look, the official ClickHouse C# driver works, but it's got some issues. It's built on ADO.NET (which... nobody really uses in 2025), copies data all over the place instead of being truly column-oriented, only supports HTTP (no native protocol), and can't do Native AOT compilation.

I needed something faster for production workloads, so I built this. It uses ClickHouse's native protocol format over gRPC and is designed around column-oriented bulk operations from the ground up.

**What you get:**
- Native protocol format via gRPC (planning to switch to bare TCP for even more speed)
- Column-oriented API built for bulk inserts and reads
- Zero-allocation design using `ArrayPool<T>` — minimal GC pressure
- 4-10x faster than official driver with 5x less memory allocation
- AOT-ready (works with Native AOT compilation)

> **⚠️ Production Warning:** Parts of this codebase were vibecoded (written fast and loose). There are probably bugs lurking around. Don't blindly deploy this to production without thorough testing in your specific environment. Run your own tests, benchmark your workloads, and make sure it actually works for your use case first.

## Performance

Here's the deal: this driver is *fast*. Like, really fast compared to the official one.

```
BenchmarkDotNet v0.15.6, Linux NixOS 24.11 (Vicuna)
AMD Ryzen 7 8845HS w/ Radeon 780M Graphics 2.16GHz, 1 CPU, 16 logical and 8 physical cores
.NET SDK 9.0.306
  [Host]    : .NET 9.0.10 (9.0.10, 9.0.1025.47515), X64 RyuJIT x86-64-v4 DEBUG
  MediumRun : .NET 9.0.10 (9.0.10, 9.0.1025.47515), X64 RyuJIT x86-64-v4

Job=MediumRun  InvocationCount=1  IterationCount=15  
LaunchCount=2  UnrollFactor=1  WarmupCount=10  
```

| Method                                  | RowCount | Mean       | Error      | StdDev    | P90        | Gen0       | Gen1       | Allocated    |
|---------------------------------------- |--------- |-----------:|-----------:|----------:|-----------:|-----------:|-----------:|-------------:|
| **'Pure gRPC driver (Native format)'**      | **10000**    |   **5.089 ms** |  **0.7023 ms** |  **1.051 ms** |   **6.322 ms** |          **-** |          **-** |    **665.41 KB** |
| 'Official ClickHouse.Client (BulkCopy)' | 10000    |  21.735 ms |  1.5206 ms |  2.229 ms |  23.752 ms |          - |          - |   3540.16 KB |
| **'Pure gRPC driver (Native format)'**      | **100000**   |  **42.736 ms** |  **3.8678 ms** |  **5.789 ms** |  **49.130 ms** |          **-** |          **-** |   **6517.27 KB** |
| 'Official ClickHouse.Client (BulkCopy)' | 100000   | 102.548 ms |  1.8689 ms |  2.739 ms | 105.010 ms |  3000.0000 |  2000.0000 |   29175.7 KB |
| **'Pure gRPC driver (Native format)'**      | **1000000**  | **317.161 ms** | **18.6165 ms** | **27.288 ms** | **343.266 ms** |          **-** |          **-** | **159327.81 KB** |
| 'Official ClickHouse.Client (BulkCopy)' | 1000000  | 925.519 ms | 33.0276 ms | 49.434 ms | 983.338 ms | 33000.0000 | 20000.0000 |  290725.3 KB |

**TL;DR:**
- ~4x faster on small batches (10k rows), up to 10x faster on larger ones
- 5x less memory allocation
- Almost zero GC pressure (notice those empty Gen0/Gen1 columns? yeah.)

Check out more detailed benchmark results in the [CI runs](https://github.com/clickhouse-pure-csharp/clickhouse-pure-csharp/actions).

## Supported Types

### Textual Encodings
* `String` → `string`
* `FixedString(N)` → `string` (trims trailing zero bytes)
* `LowCardinality(String)` → `string`
* `Nullable(String)` → `string?`

### Date and Time Encodings
* `Date` → `DateOnly` (days since 1970-01-01)
* `Date32` → `DateOnly` (days since 1900-01-01)
* `DateTime64(scale, timezone)` → `DateTimeOffset`

### Integer Encodings
* `Bool` → `bool`
* `Int8` → `sbyte`
* `Int16` → `short`
* `Int32` → `int`
* `Int64` → `long`
* `Int128` → `Int128`
* `UInt8` → `byte`
* `UInt16` → `ushort`
* `UInt32` → `uint`
* `UInt64` → `ulong`
* `UInt128` → `UInt128`
* `IPv4` → `System.Net.IPAddress`

### Floating-Point Encodings
* `Float32` → `float`
* `Float64` → `double`

## Usage Examples

### Setup Connection

```csharp
using Clickhouse.Pure.Grpc;

var router = ClickhouseGrpcConnectionFactory.Create(
    endpoint: "http://localhost:9100",
    username: "default",
    password: "",
    port: 9100,
    useSsl: false
);

var handler = new DefaultCallHandler(router);
```

### Writing Data (Bulk Insert)

The driver uses a column-oriented API for inserts. You write entire columns at once, which is way faster than row-by-row inserts.

```csharp
using Clickhouse.Pure.Columns;

// Prepare your data as arrays
var ids = new uint[] { 1, 2, 3, 4, 5 };
var names = new string[] { "Alice", "Bob", "Charlie", "Diana", "Eve" };
var timestamps = new DateTimeOffset[]
{
    DateTimeOffset.UtcNow,
    DateTimeOffset.UtcNow.AddMinutes(-5),
    DateTimeOffset.UtcNow.AddMinutes(-10),
    DateTimeOffset.UtcNow.AddMinutes(-15),
    DateTimeOffset.UtcNow.AddMinutes(-20)
};

// Create a block writer
using var writer = new NativeFormatBlockWriter(
    columnsCount: 3,
    rowsCount: ids.Length
);

// Write columns (order matters!)
writer.CreateUInt32ColumnWriter("id").WriteAll(ids);
writer.CreateStringColumnWriter("name").WriteAll(names);
writer.CreateDateTime64ColumnWriter("created_at", scale: 6, timezone: "UTC").WriteAll(timestamps);

// Send to ClickHouse
var bulkWriter = await handler.InputBulk("INSERT INTO my_table FORMAT Native");
try
{
    await bulkWriter.WriteRowsBulkAsync(writer.GetWrittenBuffer(), hasMoreData: false);
    var result = await bulkWriter.Commit();
    
    if (result.IsFailed())
    {
        throw result.Exception!;
    }
}
finally
{
    bulkWriter.Dispose();
}
```

### Reading Data (Bulk Read)

Reading is also column-oriented. You read entire columns at once, which maps perfectly to ClickHouse's storage model.

```csharp
using var reader = await handler.QueryNativeBulk(
    "SELECT id, name, created_at FROM my_table WHERE id > 100 LIMIT 10000"
);

var ids = new List<uint>();
var names = new List<string>();
var timestamps = new List<DateTimeOffset>();

while (true)
{
    var response = await reader.ReadNext();
    
    if (response.IsFailed())
    {
        throw response.Exception!;
    }
    
    if (response.Completed)
    {
        break;
    }
    
    if (!response.IsBlock())
    {
        continue; // Skip metadata/progress packets
    }
    
    // Read columns in order
    var idColumn = response.BlockReader.ReadUInt32Column();
    while (idColumn.HasMoreRows())
    {
        ids.Add(idColumn.ReadNext());
    }
    
    var nameColumn = response.BlockReader.ReadStringColumn();
    while (nameColumn.HasMoreRows())
    {
        names.Add(nameColumn.ReadNext());
    }
    
    var timestampColumn = response.BlockReader.ReadDateTime64Column();
    while (timestampColumn.HasMoreRows())
    {
        timestamps.Add(timestampColumn.ReadNext());
    }
}

// Now you have your data in parallel arrays
Console.WriteLine($"Read {ids.Count} rows");
```

### Simple Query (Non-bulk)

Sometimes you just want to run a quick query:

```csharp
var (result, exception) = await handler.QueryRawString("SELECT version()");
if (exception != null)
{
    throw exception;
}

Console.WriteLine($"ClickHouse version: {result}");
```

## What's Next?

This driver is production-ready for the types I use day-to-day, but there's more I want to add:

- **More data types** — Currently only supports types I actually use in production. Arrays, decimals, tuples, enums, etc. will come as people need them (or contribute them!)
- **Bare TCP instead of gRPC** — gRPC is nice for getting things done fast, but a hand-rolled TCP implementation would be 10-20% faster. Not a huge win right now, but worth doing eventually.
- **Generated tests** — There's a lot of repetitive test code. Templates would make this cleaner and easier to maintain.

## ClickHouse Versions

This driver should work with any ClickHouse version that's currently receiving security updates. 

Check [ClickHouse's security page](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md#security-change-log-and-support) for the list of supported versions.

## Credits

Big thanks to the [ClickHouse Go driver](https://github.com/ClickHouse/clickhouse-go) team — learned a lot from their native protocol implementation.

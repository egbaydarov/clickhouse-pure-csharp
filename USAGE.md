# Usage Examples

## Setup Connection

```csharp
using System.Collections.Generic;
using Clickhouse.Pure.Grpc;

var router = ClickhouseGrpcConnectionFactory.Create(
    endpoint: "http://127.0.0.1:9100",
    bootstrapUsername: "default",
    bootstrapPassword: "",
    port: 9100,
    useSsl: false);

using var handler = new CompressingCallHandler(
    router: router,
    password: "",
    username: "default",
    defaultSettings: new Dictionary<string, string> { { "insert_quorum", "auto" } });
```

## Writing Data (Bulk Insert)

The driver uses a column-oriented API for inserts. You write entire columns at once, which is way faster than row-by-row inserts. Use `BulkWriter.WriteNext` to send one or more native blocks before calling `Commit` to finalize the insert.

```csharp
using System;
using Clickhouse.Pure.Columns;
using Clickhouse.Pure.Grpc;

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

// Write columns (order not matter)
// Each column have seprate buffer
// It can be materialized on create or after writting all values (depending on type)
writer.CreateUInt32ColumnWriter("id").WriteAll(ids);
writer.CreateStringColumnWriter("name").WriteAll(names);
writer.CreateDateTime64ColumnWriter("created_at", scale: 6, timezone: "UTC").WriteAll(timestamps);

// Send to ClickHouse
var bulkWriter = await handler.InputBulk(
    initialQuery: "INSERT INTO my_table FORMAT Native",
    database: "default");

try
{
    var writeError = await bulkWriter.WriteNext(
        inputData: writer.GetWrittenBuffer(), // on that call all column buffers concatenated
        hasMoreData: false);

    // depending on your need (service model)
    // you can write all data in single logical block or many
    // clickhouse buffers that data on server side
    if (writeError != null)
    {
        throw new InvalidOperationException(writeError.Message);
    }

    // its mandatory to commit all written data
    // on that call clickhouse flushes buffer from memory (server side)
    // and tried to execute query
    // that call can be long
    var (progress, commitError) = await bulkWriter.Commit();
    if (commitError != null)
    {
        throw new InvalidOperationException(commitError.Message);
    }

    // if there is no errors you will receive stats
    Console.WriteLine($"Written {progress.WrittenRows} rows ({progress.WrittenBytes} bytes).");
}
finally
{
    // the API is in error as value style it doesnt throw exceptions
    // (but there are can be some bugs or uncovered cases)
    bulkWriter.Dispose();
}
```

## Reading Data (Bulk Read)

Reading is also column-oriented. `BulkReader.Read()` yields native blocks until the stream finishes, which maps perfectly to ClickHouse's storage model.

```csharp
using System;
using System.Collections.Generic;
using Clickhouse.Pure.Grpc;

using var reader = await handler.QueryNativeBulk(
    "SELECT id, name, created_at FROM my_table WHERE id > 100 LIMIT 10000");

var ids = new List<uint>();
var names = new List<string>();
var timestamps = new List<DateTimeOffset>();

// this is place where you can easy mess an order of columns
// currently working on how to make that API more obvious but still fast
while (await reader.Read() is { } block)
{
    // Read columns in order
    var idColumn = block.ReadUInt32Column();
    while (idColumn.HasMoreRows())
    {
        ids.Add(idColumn.ReadNext());
    }

    var nameColumn = block.ReadStringColumn();
    while (nameColumn.HasMoreRows())
    {
        names.Add(nameColumn.ReadNext());
    }

    var timestampColumn = block.ReadDateTime64Column();
    while (timestampColumn.HasMoreRows())
    {
        timestamps.Add(timestampColumn.ReadNext());
    }
}

var (progress, readError) = reader.GetState();
if (readError != null)
{
    throw new InvalidOperationException(readError.Message);
}

// same as for writer errors returned as value (don't forget to check it)
Console.WriteLine($"Read {progress.ReadRows} rows ({progress.ReadBytes} bytes).");
```

## Simple Query (Non-bulk)

Sometimes you just want to run a quick query (for test or debug):

```csharp
var (result, exception) = await handler.QueryRawString("SELECT version()");
if (exception != null)
{
    throw exception;
}

Console.WriteLine($"ClickHouse version: {result}");
```


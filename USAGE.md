# Usage Examples

## Setup Connection

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

## Writing Data (Bulk Insert)

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

## Reading Data (Bulk Read)

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

## Simple Query (Non-bulk)

Sometimes you just want to run a quick query:

```csharp
var (result, exception) = await handler.QueryRawString("SELECT version()");
if (exception != null)
{
    throw exception;
}

Console.WriteLine($"ClickHouse version: {result}");
```


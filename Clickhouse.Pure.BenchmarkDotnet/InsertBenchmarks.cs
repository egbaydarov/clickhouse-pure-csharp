using System.Collections.Concurrent;
using System.Numerics;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Clickhouse.Pure.Columns;
using Clickhouse.Pure.Grpc;
using FastMember;
using Grpc.Core;
using Perfolizer.Horology;

namespace Clickhouse.Pure.BenchmarkDotnet;

[Config(typeof(InsertBenchmarkConfig))]
[MemoryDiagnoser]
public class InsertBenchmarks
{
    private BenchmarkDataset _dataset = null!;
    private BenchmarkOptions _options = null!;
    private DefaultCallHandler? _pureDriver;
    private ClickHouseConnection? _officialConnection;

    private string _pureDriverTable = string.Empty;
    private string _officialDriverTable = string.Empty;

    [Params(10_000, 100_000, 1_000_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _options = BenchmarkOptions.Load();
        _dataset = BenchmarkDatasetCache.Get(RowCount);

        _pureDriver = InsertBenchmarksHelper.CreatePureDriver(_options);
        _officialConnection = await InsertBenchmarksHelper.CreateOfficialConnectionAsync(_options).ConfigureAwait(false);

        _pureDriverTable = $"{_options.Database}.benchmark_pure_{RowCount}_{Guid.NewGuid():N}";
        _officialDriverTable = $"{_options.Database}.benchmark_official_{RowCount}_{Guid.NewGuid():N}";

        await CreateBenchmarkTableAsync(_pureDriverTable).ConfigureAwait(false);
        await CreateBenchmarkTableAsync(_officialDriverTable).ConfigureAwait(false);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        BenchmarkDotNet.Helpers.AwaitHelper.GetResult(ResetTablesAsync());
    }

    [Benchmark(Description = "Pure gRPC driver (Native format)")]
    public async Task PureDriver_NativeBulkInsert()
    {
        using var blockWriter = new NativeFormatBlockWriter(columnsCount: 5, rowsCount: RowCount);
        blockWriter.CreateStringColumnWriter(nameof(BenchmarkRow.StringValue)).WriteAll(_dataset.Strings);
        blockWriter.CreateInt128ColumnWriter(nameof(BenchmarkRow.Int128Value)).WriteAll(_dataset.Int128s);
        blockWriter.CreateInt64ColumnWriter(nameof(BenchmarkRow.Int64Value)).WriteAll(_dataset.Int64s);
        blockWriter.CreateFixedStringColumnWriter(nameof(BenchmarkRow.FixedStringValue), size: 16).WriteAll(_dataset.FixedStrings);
        blockWriter.CreateLowCardinalityStringColumnWriter(nameof(BenchmarkRow.LowCardinalityValue)).WriteAll(_dataset.LowCardinalityStrings);

        var payload = blockWriter.GetWrittenBuffer();

        var bulkWriter = await _pureDriver!.InputBulk($"INSERT INTO {_pureDriverTable} FORMAT Native").ConfigureAwait(false);
        try
        {
            var wrote = await bulkWriter.WriteRowsBulkAsync(payload, hasMoreData: false).ConfigureAwait(false);
            if (!wrote)
            {
                throw new InvalidOperationException("Native bulk writer rejected payload.");
            }

            var commit = await bulkWriter.Commit().ConfigureAwait(false);
            if (commit.IsFailed())
            {
                throw commit.Exception!;
            }
        }
        finally
        {
            bulkWriter.Dispose();
        }
    }

    private async Task ResetTablesAsync()
    {
        await InsertBenchmarksHelper.TruncateTableAsync(_pureDriver!, _pureDriverTable, () => CreateBenchmarkTableAsync(_pureDriverTable)).ConfigureAwait(false);
        await InsertBenchmarksHelper.TruncateTableAsync(_pureDriver!, _officialDriverTable, () => CreateBenchmarkTableAsync(_officialDriverTable)).ConfigureAwait(false);
    }

    [Benchmark(Description = "Official ClickHouse.Client (BulkCopy)")]
    public async Task OfficialDriver_BulkCopyInsert()
    {
        var bulkCopy = new ClickHouseBulkCopy(_officialConnection!)
        {
            DestinationTableName = _officialDriverTable,
            BatchSize = RowCount,
            ColumnNames =
            [
                nameof(BenchmarkRow.StringValue),
                nameof(BenchmarkRow.Int128Value),
                nameof(BenchmarkRow.Int64Value),
                nameof(BenchmarkRow.FixedStringValue),
                nameof(BenchmarkRow.LowCardinalityValue)
            ]
        };

        await bulkCopy.InitAsync().ConfigureAwait(false);

        using var reader = ObjectReader.Create(
            _dataset.Rows,
            nameof(BenchmarkRow.StringValue),
            nameof(BenchmarkRow.Int128Value),
            nameof(BenchmarkRow.Int64Value),
            nameof(BenchmarkRow.FixedStringValue),
            nameof(BenchmarkRow.LowCardinalityValue));

        await bulkCopy.WriteToServerAsync(reader).ConfigureAwait(false);
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (_pureDriver is not null)
        {
            try
            {
                await InsertBenchmarksHelper.ExecuteAsync(_pureDriver, $"DROP TABLE IF EXISTS {_pureDriverTable}").ConfigureAwait(false);
                await InsertBenchmarksHelper.ExecuteAsync(_pureDriver, $"DROP TABLE IF EXISTS {_officialDriverTable}").ConfigureAwait(false);
            }
            catch
            {
                // ignore cleanup failures
            }

            _pureDriver.Dispose();
        }

        _officialConnection?.Dispose();
    }

    private async Task CreateBenchmarkTableAsync(string tableName)
    {
        await InsertBenchmarksHelper.ExecuteAsync(_pureDriver!, $"DROP TABLE IF EXISTS {tableName}").ConfigureAwait(false);

        var createSql = $"""
                         CREATE TABLE {tableName}
                         (
                             StringValue String,
                             Int128Value Int128,
                             Int64Value Int64,
                             FixedStringValue FixedString(16),
                             LowCardinalityValue LowCardinality(String)
                         )
                         ENGINE = MergeTree
                         ORDER BY tuple()
                         """;

        await InsertBenchmarksHelper.ExecuteAsync(_pureDriver!, createSql).ConfigureAwait(false);
    }
}

[Config(typeof(InsertBenchmarkConfig))]
[MemoryDiagnoser]
public class SingleColumnInsertBenchmarks
{
    private BenchmarkOptions _options = null!;
    private BenchmarkDataset _dataset = null!;
    private DefaultCallHandler? _pureDriver;
    private ClickHouseConnection? _officialConnection;

    private string _pureDriverTable = string.Empty;
    private string _officialDriverTable = string.Empty;

    [Params(10_000, 100_000, 1_000_000)]
    public int RowCount { get; set; }

    [ParamsSource(nameof(ColumnCases))]
    public ColumnCase Column { get; set; } = ColumnCase.All[0];

    public static IEnumerable<ColumnCase> ColumnCases => ColumnCase.All;

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _options = BenchmarkOptions.Load();
        _dataset = BenchmarkDatasetCache.Get(RowCount);

        _pureDriver = InsertBenchmarksHelper.CreatePureDriver(_options);
        _officialConnection = await InsertBenchmarksHelper.CreateOfficialConnectionAsync(_options).ConfigureAwait(false);

        var suffix = $"{SanitizeIdentifier(Column.Name)}_{RowCount}_{Guid.NewGuid():N}";
        _pureDriverTable = $"{_options.Database}.benchmark_single_pure_{suffix}";
        _officialDriverTable = $"{_options.Database}.benchmark_single_official_{suffix}";

        await CreateBenchmarkTableAsync(_pureDriverTable).ConfigureAwait(false);
        await CreateBenchmarkTableAsync(_officialDriverTable).ConfigureAwait(false);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        BenchmarkDotNet.Helpers.AwaitHelper.GetResult(ResetTablesAsync());
    }
    private async Task ResetTablesAsync()
    {
        await InsertBenchmarksHelper.TruncateTableAsync(_pureDriver!, _pureDriverTable, () => CreateBenchmarkTableAsync(_pureDriverTable)).ConfigureAwait(false);
        await InsertBenchmarksHelper.TruncateTableAsync(_pureDriver!, _officialDriverTable, () => CreateBenchmarkTableAsync(_officialDriverTable)).ConfigureAwait(false);
    }


    [Benchmark(Description = "Pure gRPC driver (Native format)")]
    public async Task PureDriver_SingleColumn()
    {
        using var blockWriter = new NativeFormatBlockWriter(columnsCount: 1, rowsCount: RowCount);
        Column.WriteNative(blockWriter, _dataset);
        var payload = blockWriter.GetWrittenBuffer();

        var bulkWriter = await _pureDriver!.InputBulk($"INSERT INTO {_pureDriverTable} FORMAT Native").ConfigureAwait(false);
        try
        {
            var wrote = await bulkWriter.WriteRowsBulkAsync(payload, hasMoreData: false).ConfigureAwait(false);
            if (!wrote)
            {
                throw new InvalidOperationException("Native bulk writer rejected payload.");
            }

            var commit = await bulkWriter.Commit().ConfigureAwait(false);
            if (commit.IsFailed())
            {
                throw commit.Exception!;
            }
        }
        finally
        {
            bulkWriter.Dispose();
        }
    }

    [Benchmark(Description = "Official ClickHouse.Client (BulkCopy)")]
    public async Task OfficialDriver_SingleColumn()
    {
        var bulkCopy = new ClickHouseBulkCopy(_officialConnection!)
        {
            DestinationTableName = _officialDriverTable,
            BatchSize = RowCount,
            ColumnNames = new[] { Column.ColumnName }
        };

        await bulkCopy.InitAsync().ConfigureAwait(false);

        using var reader = ObjectReader.Create(_dataset.Rows, Column.PropertyName);
        await bulkCopy.WriteToServerAsync(reader).ConfigureAwait(false);
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (_pureDriver is not null)
        {
            try
            {
                await InsertBenchmarksHelper.ExecuteAsync(_pureDriver, $"DROP TABLE IF EXISTS {_pureDriverTable}").ConfigureAwait(false);
                await InsertBenchmarksHelper.ExecuteAsync(_pureDriver, $"DROP TABLE IF EXISTS {_officialDriverTable}").ConfigureAwait(false);
            }
            catch
            {
                // ignore cleanup failures
            }

            _pureDriver.Dispose();
        }

        _officialConnection?.Dispose();
    }

    private async Task CreateBenchmarkTableAsync(string tableName)
    {
        await InsertBenchmarksHelper.ExecuteAsync(_pureDriver!, $"DROP TABLE IF EXISTS {tableName}").ConfigureAwait(false);

        var createSql = $"""
                         CREATE TABLE {tableName}
                         (
                             {Column.ColumnName} {Column.ClickHouseType}
                         )
                         ENGINE = MergeTree
                         ORDER BY tuple()
                         """;

        await InsertBenchmarksHelper.ExecuteAsync(_pureDriver!, createSql).ConfigureAwait(false);
    }

    private static string SanitizeIdentifier(string value)
    {
        Span<char> buffer = stackalloc char[value.Length];
        var length = 0;
        foreach (var ch in value)
        {
            buffer[length++] = char.IsLetterOrDigit(ch) ? char.ToLowerInvariant(ch) : '_';
        }

        return new string(buffer[..length]).Trim('_');
    }
}

internal sealed class InsertBenchmarkConfig : ManualConfig
{
    public InsertBenchmarkConfig()
    {
        WithArtifactsPath(Path.Combine(AppContext.BaseDirectory, "BenchmarkDotNet-Artifacts"));
        AddJob(Job.MediumRun);

        AddColumn(TargetMethodColumn.Method, StatisticColumn.Mean, StatisticColumn.P90, StatisticColumn.Error);
        AddDiagnoser(BenchmarkDotNet.Diagnosers.MemoryDiagnoser.Default);
        AddLogger(ConsoleLogger.Default);
        AddExporter(MarkdownExporter.Console);
        AddAnalyser(EnvironmentAnalyser.Default);
        Options = ConfigOptions.DisableOptimizationsValidator;
    }
}

internal static class InsertBenchmarksHelper
{
    internal static DefaultCallHandler CreatePureDriver(BenchmarkOptions options)
    {
        return new DefaultCallHandler(
            router: new ClickHouseGrpcRouter(
                seedEndpoints: [options.GrpcSeedEndpoint],
                port: options.GrpcPort,
                username: options.Username,
                password: options.Password,
                useSsl: options.GrpcUseSsl),
            password: options.Password,
            username: options.Username,
            queryTimeout: TimeSpan.FromMinutes(5));
    }

    internal static async Task<ClickHouseConnection> CreateOfficialConnectionAsync(BenchmarkOptions options)
    {
        var connection = new ClickHouseConnection(options.BuildHttpConnectionString());
        await connection.OpenAsync().ConfigureAwait(false);
        return connection;
    }

    internal static async Task ExecuteAsync(DefaultCallHandler handler, string sql)
    {
        var (_, error) = await handler.QueryRawString(sql).ConfigureAwait(false);
        if (error is not null)
        {
            throw error;
        }
    }

    internal static async Task TruncateTableAsync(DefaultCallHandler handler, string tableName, Func<Task> recreateTableAsync)
    {
        try
        {
            await ExecuteAsync(handler, $"TRUNCATE TABLE {tableName}").ConfigureAwait(false);
        }
        catch (RpcException)
        {
            await ExecuteAsync(handler, $"DROP TABLE IF EXISTS {tableName}").ConfigureAwait(false);
            await recreateTableAsync().ConfigureAwait(false);
        }
    }
}

public sealed record ColumnCase(
    string Name,
    string ColumnName,
    string ClickHouseType,
    Action<NativeFormatBlockWriter, BenchmarkDataset> WriteNative,
    string PropertyName)
{
    public override string ToString() => Name;

    public static readonly ColumnCase[] All =
    [
        new(
            "String",
            nameof(BenchmarkRow.StringValue),
            "String",
            (writer, data) => writer.CreateStringColumnWriter(nameof(BenchmarkRow.StringValue)).WriteAll(data.Strings),
            nameof(BenchmarkRow.StringValue)),
        new(
            "Int64",
            nameof(BenchmarkRow.Int64Value),
            "Int64",
            (writer, data) => writer.CreateInt64ColumnWriter(nameof(BenchmarkRow.Int64Value)).WriteAll(data.Int64s),
            nameof(BenchmarkRow.Int64Value)),
        new(
            "Int128",
            nameof(BenchmarkRow.Int128Value),
            "Int128",
            (writer, data) => writer.CreateInt128ColumnWriter(nameof(BenchmarkRow.Int128Value)).WriteAll(data.Int128s),
            nameof(BenchmarkRow.Int128Value)),
        new(
            "FixedString(16)",
            nameof(BenchmarkRow.FixedStringValue),
            "FixedString(16)",
            (writer, data) => writer.CreateFixedStringColumnWriter(nameof(BenchmarkRow.FixedStringValue), 16).WriteAll(data.FixedStrings),
            nameof(BenchmarkRow.FixedStringValue)),
        new(
            "LowCardinality(String)",
            nameof(BenchmarkRow.LowCardinalityValue),
            "LowCardinality(String)",
            (writer, data) => writer.CreateLowCardinalityStringColumnWriter(nameof(BenchmarkRow.LowCardinalityValue)).WriteAll(data.LowCardinalityStrings),
            nameof(BenchmarkRow.LowCardinalityValue)),
    ];
}

public sealed class BenchmarkDataset
{
    internal BenchmarkDataset(
        IReadOnlyList<BenchmarkRow> rows,
        string[] strings,
        Int128[] int128s,
        long[] int64s,
        string[] fixedStrings,
        string[] lowCardinalityStrings)
    {
        Rows = rows;
        Strings = strings;
        Int128s = int128s;
        Int64s = int64s;
        FixedStrings = fixedStrings;
        LowCardinalityStrings = lowCardinalityStrings;
    }

    internal IReadOnlyList<BenchmarkRow> Rows { get; }
    internal string[] Strings { get; }
    internal Int128[] Int128s { get; }
    internal long[] Int64s { get; }
    internal string[] FixedStrings { get; }
    internal string[] LowCardinalityStrings { get; }
}

internal static class BenchmarkDatasetCache
{
    private static readonly ConcurrentDictionary<int, BenchmarkDataset> Cache = new();

    internal static BenchmarkDataset Get(int rowCount)
    {
        return Cache.GetOrAdd(rowCount, CreateDataset);
    }

    private static BenchmarkDataset CreateDataset(int rowCount)
    {
        var rows = DataFactory.CreateRows(rowCount);
        var strings = rows.Select(r => r.StringValue).ToArray();
        var int128s = rows.Select(r => r.NativeInt128).ToArray();
        var int64s = rows.Select(r => r.Int64Value).ToArray();
        var fixedStrings = rows.Select(r => r.FixedStringValue).ToArray();
        var lowCardinalityStrings = rows.Select(r => r.LowCardinalityValue).ToArray();

        return new BenchmarkDataset(rows, strings, int128s, int64s, fixedStrings, lowCardinalityStrings);
    }
}

internal sealed record BenchmarkRow(
    string StringValue,
    BigInteger Int128Value,
    long Int64Value,
    string FixedStringValue,
    string LowCardinalityValue)
{
    public Int128 NativeInt128 { get; } = Int128.CreateChecked(Int128Value);
}

internal sealed record BenchmarkOptions(
    string HttpHost,
    ushort HttpPort,
    string HttpProtocol,
    string GrpcHost,
    ushort GrpcPort,
    bool GrpcUseSsl,
    string Database,
    string Username,
    string Password)
{
    public static BenchmarkOptions Load()
    {
        const string defaultHost = "127.0.0.1";
        var httpHost = Environment.GetEnvironmentVariable("CLICKHOUSE_HTTP_HOST") ?? defaultHost;
        var httpPort = ParsePort("CLICKHOUSE_HTTP_PORT", 8123);
        var httpProtocol = Environment.GetEnvironmentVariable("CLICKHOUSE_HTTP_PROTOCOL") ?? "http";

        var grpcHost = Environment.GetEnvironmentVariable("CLICKHOUSE_GRPC_HOST") ?? defaultHost;
        var grpcPort = ParsePort("CLICKHOUSE_GRPC_PORT", 9100);
        var grpcUseSsl = bool.TryParse(Environment.GetEnvironmentVariable("CLICKHOUSE_GRPC_SSL"), out var ssl) && ssl;

        var database = Environment.GetEnvironmentVariable("CLICKHOUSE_DATABASE") ?? "default";
        var username = Environment.GetEnvironmentVariable("CLICKHOUSE_USER") ?? "default";
        var password = Environment.GetEnvironmentVariable("CLICKHOUSE_PASSWORD") ?? "default";

        return new BenchmarkOptions(
            httpHost,
            httpPort,
            httpProtocol,
            grpcHost,
            grpcPort,
            grpcUseSsl,
            database,
            username,
            password);
    }

    public string BuildHttpConnectionString()
    {
        return $"Host={HttpHost};Port={HttpPort};Database={Database};User={Username};Password={Password};Protocol={HttpProtocol};Compression=lz4;";
    }

    public string GrpcSeedEndpoint => $"{(GrpcUseSsl ? "https" : "http")}://{GrpcHost}:{GrpcPort}";

    private static ushort ParsePort(string variableName, ushort fallback)
    {
        var value = Environment.GetEnvironmentVariable(variableName);
        if (ushort.TryParse(value, out var parsed))
        {
            return parsed;
        }

        return fallback;
    }
}

internal static class DataFactory
{
    private static readonly string[] Categories = ["alpha", "beta", "gamma", "delta", "epsilon"];

    public static List<BenchmarkRow> CreateRows(int count)
    {
        var random = new Random(42);
        var rows = new List<BenchmarkRow>(count);

        for (var i = 0; i < count; i++)
        {
            var name = $"row_{i:D8}";
            var int64Value = random.NextInt64(long.MinValue / 2, long.MaxValue / 2);

            var high = (Int128)random.NextInt64();
            var low = (Int128)(ulong)random.NextInt64();
            var int128 = (high << 64) ^ low;
            var bigInteger = BigInteger.CreateChecked(int128);

            var fixedString = $"FS_{i:D13}";
            var category = Categories[i % Categories.Length];

            rows.Add(new BenchmarkRow(
                name,
                bigInteger,
                int64Value,
                fixedString,
                category));
        }

        return rows;
    }
}

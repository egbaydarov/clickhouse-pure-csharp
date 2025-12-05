using System.Globalization;
using System.Text;

namespace Clickhouse.Pure.Grpc.Tests;

public class Sut
{
    private readonly CompressingCallHandler _handler;

    public Sut(
        CompressingCallHandler handler)
    {
        _handler = handler;
    }

    public async Task InsertCsvAsync(
        string tableName,
        IEnumerable<string> rows)
    {
        if (string.IsNullOrWhiteSpace(value: tableName))
        {
            throw new ArgumentException(message: "Table name must be provided", paramName: nameof(tableName));
        }

        ArgumentNullException.ThrowIfNull(argument: rows);
        var rowList = rows as IList<string> ?? rows.ToList();

        if (rowList.Count == 0)
        {
            throw new ArgumentException(message: "At least one row must be provided.", paramName: nameof(rows));
        }

        var bulkWriter = await _handler.InputBulk(
            initialQuery: $"INSERT INTO {tableName} FORMAT CSV",
            inputDataDelimiter: "\n");

        try
        {
            for (var i = 0; i < rowList.Count; i++)
            {
                var payload = Encoding.UTF8.GetBytes(s: rowList[index: i]);
                var hasMore = i < rowList.Count - 1;

                var error = await bulkWriter.WriteNext(inputData: payload, hasMoreData: hasMore);
                if (error != null)
                {
                    throw new InvalidOperationException(error.Message);
                }
            }

            var commit = await bulkWriter.Commit();
            if (commit.Error != null)
            {
                throw new System.Exception(commit.Error.Message);
            }
        }
        finally
        {
            bulkWriter.Dispose();
        }
    }

    public async Task<string?> GetVersionThrowing()
    {
        var (res, ex) = await _handler.QueryRawString("SELECT VERSION()");
        if (ex != null)
        {
            throw ex;
        }

        return res;
    }

    public async Task<Result> GetVersionResultThrowing()
    {
        var (res, ex) = await _handler.QueryRawResult("SELECT VERSION()");
        if (ex != null)
        {
            throw ex;
        }

        return res!;
    }

    public async Task<string> PrepareBulkTableAsync(
        string? tableName = null)
    {
        tableName ??= $"default.default_call_handler_bulk_{Guid.NewGuid():N}";

        var (_, dropEx) = await _handler.QueryRawString($"DROP TABLE IF EXISTS {tableName}");
        if (dropEx != null)
        {
            throw dropEx;
        }

        var (_, createEx) = await _handler.QueryRawString($"CREATE TABLE {tableName} (value UInt32) ENGINE = MergeTree ORDER BY tuple()");
        if (createEx != null)
        {
            throw createEx;
        }

        return tableName;
    }

    public async Task<(WriteProgress Progress, WriteError? Error)> InsertBulkNumbersAsync(
        string tableName,
        params uint[] values)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name must be provided", nameof(tableName));
        }

        if (values.Length == 0)
        {
            throw new ArgumentException("At least one value is required", nameof(values));
        }

        var bulkWriter = await _handler.InputBulk(
            initialQuery: $"INSERT INTO {tableName} FORMAT CSV",
            database: "default",
            inputDataDelimiter: "\n");

        try
        {
            for (var i = 0; i < values.Length; i++)
            {
                var payload = Encoding.UTF8.GetBytes(values[i].ToString(CultureInfo.InvariantCulture));
                var hasMore = i < values.Length - 1;

                var error = await bulkWriter.WriteNext(payload, hasMore);
                if (error != null)
                {
                    break;
                }
            }

            return await bulkWriter.Commit();
        }
        finally
        {
            bulkWriter.Dispose();
        }
    }

    public async Task<IReadOnlyList<uint>> FetchInsertedNumbersAsync(
        string tableName)
    {
        var (result, ex) = await _handler.QueryRawResult($"SELECT value FROM {tableName} ORDER BY value FORMAT TabSeparated");
        if (ex != null)
        {
            throw ex;
        }

        if (result == null
            || result.Output.IsEmpty)
        {
            return [];
        }

        var payload = Encoding.UTF8.GetString(result.Output.Span);
        var values = payload
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(v => uint.Parse(v, CultureInfo.InvariantCulture))
            .ToArray();

        return values;
    }

    public Task<BulkReader> QueryNativeBulkAsync(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
        {
            throw new ArgumentException("Query must be provided.", nameof(query));
        }

        return _handler.QueryNativeBulk(query);
    }

    public async Task DropTableAsync(
        string tableName)
    {
        var (_, ex) = await _handler.QueryRawString($"DROP TABLE IF EXISTS {tableName}");
        if (ex != null)
        {
            throw ex;
        }
    }

    public async Task CreateSingleColumnTableAsync(
        string tableName,
        string clickhouseType)
    {
        await CreateTableAsync(tableName, new[] { ("Value", clickhouseType) });
    }

    public async Task CreateTableAsync(
        string tableName,
        IReadOnlyList<(string Name, string Type)> columns)
    {
        ArgumentNullException.ThrowIfNull(columns);

        if (columns.Count == 0)
        {
            throw new ArgumentException("At least one column definition must be provided.", nameof(columns));
        }

        await DropTableAsync(tableName);

        var columnsSql = string.Join(", ", columns.Select(c => $"{c.Name} {c.Type}"));

        var createSql = $"CREATE TABLE {tableName} ({columnsSql}) ENGINE = MergeTree ORDER BY tuple()";
        var (_, createEx) = await _handler.QueryRawString(createSql);
        if (createEx != null)
        {
            throw createEx;
        }
    }

    public async Task InsertNativePayloadAsync(
        string tableName,
        ReadOnlyMemory<byte> payload)
    {
        var bulkWriter = await _handler.InputBulk($"INSERT INTO {tableName} FORMAT Native");
        try
        {
            var error = await bulkWriter.WriteNext(payload, hasMoreData: false);
            if (error != null)
            {
                throw new InvalidOperationException(error.Message);
            }

            var commit = await bulkWriter.Commit();
            if (commit.Error != null)
            {
                throw new System.Exception(commit.Error.Message);
            }
        }
        finally
        {
            bulkWriter.Dispose();
        }
    }

    public async Task InsertNativePayloadParallel(
        string tableName1,
        string tableName2,
        ReadOnlyMemory<byte> payload1,
        ReadOnlyMemory<byte> payload2)
    {
        var bulkWriter1 = await _handler.InputBulk($"INSERT INTO {tableName1} FORMAT Native");
        var bulkWriter2 = await _handler.InputBulk($"INSERT INTO {tableName2} FORMAT Native");
        try
        {
            var error1 = await bulkWriter1.WriteNext(payload1, hasMoreData: false);
            var error2 = await bulkWriter2.WriteNext(payload2, hasMoreData: false);
            if (error1 != null)
            {
                throw new InvalidOperationException(error1.Message);
            }
            if (error2 != null)
            {
                throw new InvalidOperationException(error2.Message);
            }

            var commit2 = await bulkWriter2.Commit();
            if (commit2.Error != null)
            {
                throw new System.Exception(commit2.Error.Message);
            }
            bulkWriter2.Dispose();
            var commit1 = await bulkWriter1.Commit();
            if (commit1.Error != null)
            {
                throw new System.Exception(commit1.Error.Message);
            }

        }
        finally
        {
            bulkWriter1.Dispose();
        }
    }

    public async Task<IReadOnlyList<T>> FetchCsvColumnAsync<T>(
        string tableName,
        Func<string, T> converter)
    {
        return await FetchCsvColumnAsync(tableName, "Value", converter);
    }

    //TODO: sort in tests and here
    public async Task<IReadOnlyList<T>> FetchCsvColumnAsync<T>(
        string tableName,
        string columnExpression,
        Func<string, T> converter)
    {
        if (string.IsNullOrWhiteSpace(columnExpression))
        {
            throw new ArgumentException("Column expression must be provided", nameof(columnExpression));
        }

        var (result, ex) = await _handler.QueryRawResult($"SELECT {columnExpression} FROM {tableName} FORMAT CSV");
        if (ex != null)
        {
            throw ex;
        }

        if (result == null || result.Output.IsEmpty)
        {
            return [];
        }

        var text = Encoding.UTF8.GetString(result.Output.Span);
        var split = text.Split('\n', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        var values = new List<T>(split.Length);
        foreach (var entry in split)
        {
            var value = entry;
            if (value is ['"', _, ..] && value[^1] == '"')
            {
                // remove quotes
                // replace escaped with regular
                value = value[1..^1]
                    .Replace("\"\"", "\"");
            }
            values.Add(converter(value));
        }

        return values;
    }

    public async Task<IReadOnlyList<ulong>> Read5NumbersNativeAsync()
    {
        using var reader = await _handler.QueryNativeBulk("SELECT number FROM system.numbers LIMIT 5");
        var values = new List<ulong>();

        while (await reader.Read() is { } block)
        {
            var column = block.ReadUInt64Column();
            while (column.HasMoreRows())
            {
                values.Add(column.ReadNext());
            }
        }

        reader.GetState();

        return values;
    }
}

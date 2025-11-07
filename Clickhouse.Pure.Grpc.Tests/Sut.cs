using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Clickhouse.Pure.Grpc.Tests;

public class Sut
{
    private readonly DefaultCallHandler _handler;

    public Sut(
        DefaultCallHandler handler)
    {
        _handler = handler;
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

    public async Task<CommitBulkReponse> InsertBulkNumbersAsync(
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
            $"INSERT INTO {tableName} FORMAT CSV", "\n");

        try
        {
            for (var i = 0; i < values.Length; i++)
            {
                var payload = Encoding.UTF8.GetBytes(values[i].ToString(CultureInfo.InvariantCulture));
                var hasMore = i < values.Length - 1;

                var wrote = await bulkWriter.WriteRowsBulkAsync(payload, hasMore);
                if (!wrote)
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
            var wrote = await bulkWriter.WriteRowsBulkAsync(payload, hasMoreData: false);
            if (!wrote)
            {
                throw new InvalidOperationException("Failed to write native block payload.");
            }

            var commit = await bulkWriter.Commit();
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

    public async Task<IReadOnlyList<T>> FetchCsvColumnAsync<T>(
        string tableName,
        Func<string, T> converter)
    {
        return await FetchCsvColumnAsync(tableName, "Value", converter);
    }

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
            return Array.Empty<T>();
        }

        var text = Encoding.UTF8.GetString(result.Output.Span);
        var split = text.Split('\n', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        var values = new List<T>(split.Length);
        foreach (var entry in split)
        {
            var value = entry;
            if (value.Length > 0 && value[^1] == '\r')
            {
                value = value[..^1];
            }
            if (value is ['"', _, ..] && value[^1] == '"')
            {
                value = value[1..^1].Replace("\"\"", "\"");
            }
            values.Add(converter(value));
        }

        return values;
    }

    public async Task<IReadOnlyList<ulong>> Read5NumbersNativeAsync()
    {
        using var reader = await _handler.QueryNativeBulk("SELECT number FROM system.numbers LIMIT 5");
        var values = new List<ulong>();

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
                continue;
            }

            var column = response.BlockReader.AdvanceUInt64Column();
            while (column.HasMoreRows())
            {
                values.Add(column.GetCellValueAndAdvance());
            }
        }

        return values;
    }
}
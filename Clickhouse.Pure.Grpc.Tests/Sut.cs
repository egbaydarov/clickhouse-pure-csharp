using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Clickhouse.Pure.ColumnCodeGenerator;
using Grpc.Core;

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

        var (_, createEx) = await _handler.QueryRawString($"CREATE TABLE {tableName} (value UInt32) ENGINE = Memory");
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

        var bulkWriter = await _handler.InputBulk($"INSERT INTO {tableName} FORMAT CSV", "\n");

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
            return Array.Empty<uint>();
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

    public async Task<IReadOnlyList<ulong>> ReadNumbersNativeAsync()
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
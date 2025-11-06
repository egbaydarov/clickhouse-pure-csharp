using System.Globalization;
using AwesomeAssertions;
using Clickhouse.Pure.ColumnCodeGenerator;
using Xunit;

namespace Clickhouse.Pure.Grpc.Tests;

public class NativeFormatBlockWriterTests
{
    [Fact]
    public async Task UInt32Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_uint32_{Guid.NewGuid():N}";
        var values = LoadValues("native_uint32.csv", s => uint.Parse(s, CultureInfo.InvariantCulture));

        await sut.CreateSingleColumnTableAsync(tableName, "UInt32");

        try
        {
            var payload = BuildPayload(values.Count, writer =>
            {
                var column = writer.AdvanceUInt32ColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => uint.Parse(s, CultureInfo.InvariantCulture));

            fetched
                .Should()
                .Equal(values);
        }
        finally
        {
            await sut.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task StringColumn_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_string_{Guid.NewGuid():N}";
        var values = LoadValues("native_string.csv", s => s);

        await sut.CreateSingleColumnTableAsync(tableName, "String");

        try
        {
            var payload = BuildPayload(values.Count, writer =>
            {
                var column = writer.AdvanceStringColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                static s => s);

            fetched
                .Should()
                .Equal(values);
        }
        finally
        {
            await sut.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task LowCardinalityStringColumn_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_lowcard_{Guid.NewGuid():N}";
        var values = LoadValues("native_lowcard_string.csv", s => s);

        await sut.CreateSingleColumnTableAsync(tableName, "LowCardinality(String)");

        try
        {
            var payload = BuildPayload(values.Count, writer =>
            {
                var column = writer.AdvanceLowCardinalityStringColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
                column.GetColumnData();
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                static s => s);

            fetched
                .Should()
                .Equal(values);
        }
        finally
        {
            await sut.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task Date32Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_date32_{Guid.NewGuid():N}";
        var values = LoadValues("native_date32.csv", s => DateOnly.Parse(s, CultureInfo.InvariantCulture));

        await sut.CreateSingleColumnTableAsync(tableName, "Date32");

        try
        {
            var payload = BuildPayload(values.Count, writer =>
            {
                var column = writer.AdvanceDate32ColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => DateOnly.Parse(s, CultureInfo.InvariantCulture));

            fetched
                .Should()
                .Equal(values);
        }
        finally
        {
            await sut.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task Float64Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_float64_{Guid.NewGuid():N}";
        var values = LoadValues("native_float64.csv", s => double.Parse(s, CultureInfo.InvariantCulture));

        await sut.CreateSingleColumnTableAsync(tableName, "Float64");

        try
        {
            var payload = BuildPayload(values.Count, writer =>
            {
                var column = writer.AdvanceFloat64ColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => double.Parse(s, CultureInfo.InvariantCulture));

            fetched
                .Should()
                .Equal(values);
        }
        finally
        {
            await sut.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task BoolColumn_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_bool_{Guid.NewGuid():N}";
        var values = LoadValues("native_bool.csv", bool.Parse);

        await sut.CreateSingleColumnTableAsync(tableName, "Bool");

        try
        {
            var payload = BuildPayload(values.Count, writer =>
            {
                var column = writer.AdvanceBoolColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                bool.Parse);

            fetched
                .Should()
                .Equal(values);
        }
        finally
        {
            await sut.DropTableAsync(tableName);
        }
    }

    private static byte[] BuildPayload(
        int rowCount,
        Action<NativeFormatBlockWriter> fill)
    {
        using var writer = new NativeFormatBlockWriter(columnsCount: 1, rowsCount: (ulong)rowCount);
        fill(writer);
        return writer.GetWrittenBuffer().ToArray();
    }

    private static IReadOnlyList<T> LoadValues<T>(
        string fileName,
        Func<string, T> converter)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        return File.ReadAllLines(path)
            .Skip(1)
            .Where(line => !string.IsNullOrWhiteSpace(line))
            .Select(converter)
            .ToList();
    }
}


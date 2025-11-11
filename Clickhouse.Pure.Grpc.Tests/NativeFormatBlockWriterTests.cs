using System.Globalization;
using System.Numerics;
using System.Net;
using AwesomeAssertions;
using Clickhouse.Pure.Columns;
using Xunit;

namespace Clickhouse.Pure.Grpc.Tests;

public class NativeFormatBlockWriterTests  : IAsyncDisposable
{
    private readonly Sut _sut = SutFactory.Create();
    private string? _tableName;

    [Fact]
    public async Task UInt32Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_uint32_{Guid.NewGuid():N}";
        var values = new uint[] { 0, 1, uint.MaxValue };

        await _sut.CreateSingleColumnTableAsync(
            tableName: _tableName,
            clickhouseType: "UInt32");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateUInt32ColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => uint.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Int8Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_int8_{Guid.NewGuid():N}";
        var values = new sbyte[] { sbyte.MinValue, 0, sbyte.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Int8");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateInt8ColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => sbyte.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Int16Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_int16_{Guid.NewGuid():N}";
        var values = new short[] { short.MinValue, -1, short.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Int16");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateInt16ColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => short.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Int32Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_int32_{Guid.NewGuid():N}";
        var values = new[] { int.MinValue, 0, int.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Int32");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateInt32ColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => int.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Int64Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_int64_{Guid.NewGuid():N}";
        var values = new[] { long.MinValue, -1234567890L, long.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Int64");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateInt64ColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => long.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Int128Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_int128_{Guid.NewGuid():N}";
        var values = new[]
        {
            Int128.MinValue,
            Int128.Parse("-1234567890123456789012345678901234", CultureInfo.InvariantCulture),
            Int128.MaxValue,
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Int128");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateInt128ColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => Int128.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task UInt8Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_uint8_{Guid.NewGuid():N}";
        var values = new byte[] { 0, 1, byte.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "UInt8");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateUInt8ColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => byte.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task UInt16Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_uint16_{Guid.NewGuid():N}";
        var values = new ushort[] { 0, 1, ushort.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "UInt16");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateUInt16ColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => ushort.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task UInt64Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_uint64_{Guid.NewGuid():N}";
        var values = new[] { 0UL, 1UL, ulong.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "UInt64");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateUInt64ColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => ulong.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task UInt128Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_uint128_{Guid.NewGuid():N}";
        var values = new[]
        {
            UInt128.Zero,
            UInt128.Parse("123456789012345678901234567890123456", CultureInfo.InvariantCulture),
            UInt128.MaxValue,
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, "UInt128");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateUInt128ColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => UInt128.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task StringColumn_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_string_{Guid.NewGuid():N}";
        var values = new[] { "hello", "world", "test" };

        await _sut.CreateSingleColumnTableAsync(_tableName, "String");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateStringColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            static s => s);

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task NullableStringColumn_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_nullable_string_{Guid.NewGuid():N}";
        var values = new[] { null, "alpha", null, "beta" };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Nullable(String)");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateNullableStringColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => s == "\\N" ? null : s);

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task FixedStringColumn_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_fixed_string_{Guid.NewGuid():N}";
        var values = new[] { "short", "exact8ch", "pad" };

        await _sut.CreateSingleColumnTableAsync(_tableName, "FixedString(8)");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateFixedStringColumnWriter("Value", 8)
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => s.TrimEnd('\0'));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task LowCardinalityStringColumn_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_lowcard_{Guid.NewGuid():N}";
        var values = new[] { "alpha", "beta", "alpha", "gamma", "beta" };

        await _sut.CreateSingleColumnTableAsync(_tableName, "LowCardinality(String)");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        var col = writer.CreateLowCardinalityStringColumnWriter("Value");
        foreach (var value in values)
        {
            col.WriteNext(value);
        }
        col.GetColumnData();

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            static s => s);

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task LowCardinalityStringColumn_LargeDictionary_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_lowcard_large_{Guid.NewGuid():N}";
        var values = Enumerable.Range(0, 300).Select(i => $"Value_{i:D3}").ToList();

        await _sut.CreateSingleColumnTableAsync(_tableName, "LowCardinality(String)");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Count);

        var col = writer.CreateLowCardinalityStringColumnWriter("Value");
        foreach (var value in values)
        {
            col.WriteNext(value);
        }
        col.GetColumnData();

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            static s => s);

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Date32Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_date32_{Guid.NewGuid():N}";
        var values = new[]
        {
            new DateOnly(1900, 1, 1),
            new DateOnly(2024, 11, 9),
            new DateOnly(2299, 12, 31),
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Date32");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateDate32ColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => DateOnly.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task DateColumn_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_date_{Guid.NewGuid():N}";
        var values = new[]
        {
            new DateOnly(1970, 1, 1),
            new DateOnly(2000, 2, 29),
            new DateOnly(2106, 2, 7),
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Date");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateDateColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => DateOnly.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task DateTime64Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_datetime64_{Guid.NewGuid():N}";
        var values = new[]
        {
            new DateTimeOffset(2023, 1, 1, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2024, 5, 12, 15, 30, 45, TimeSpan.FromHours(3)),
            new DateTimeOffset(2035, 12, 31, 23, 59, 59, TimeSpan.FromHours(-4)),
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, "DateTime64(3)");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateDateTime64ColumnWriter("Value", 3, string.Empty)
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var expected = values.Select(ToUnixTimeNanoseconds).ToArray();

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            "toUnixTimestamp64Nano(Value)",
            s => long.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(expected);
    }

    [Fact]
    public async Task DateTime64Column_WithTimeZone_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_datetime64_tz_{Guid.NewGuid():N}";
        const string timeZone = "Europe/Berlin";
        var values = new[]
        {
            new DateTimeOffset(2022, 3, 27, 1, 59, 59, TimeSpan.Zero),
            new DateTimeOffset(2022, 3, 27, 2, 0, 1, TimeSpan.Zero),
            new DateTimeOffset(2022, 10, 30, 1, 30, 0, TimeSpan.Zero),
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, $"DateTime64(6, '{timeZone}')");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateDateTime64ColumnWriter("Value", 6, timeZone)
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var expected = values.Select(ToUnixTimeNanoseconds).ToArray();

        var fetchedNanos = await _sut.FetchCsvColumnAsync(
            _tableName,
            "toUnixTimestamp64Nano(Value)",
            s => long.Parse(s, CultureInfo.InvariantCulture));

        fetchedNanos
            .Should()
            .Equal(expected);

        var fetchedLocal = await _sut.FetchCsvColumnAsync(
            _tableName,
            static s => s);

        var tz = TimeZoneInfo.FindSystemTimeZoneById(timeZone);
        var reconstructed = fetchedLocal
            .Select(s => DateTime.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.None))
            .Select(local =>
            {
                var offset = tz.GetUtcOffset(local);
                var dto = new DateTimeOffset(local, offset);
                return ToUnixTimeNanoseconds(dto);
            })
            .ToArray();

        reconstructed
            .Should()
            .Equal(expected);
    }

    [Fact]
    public async Task IPv4Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_ipv4_{Guid.NewGuid():N}";
        var values = new[]
        {
            IPAddress.Parse("0.0.0.0"),
            IPAddress.Parse("10.1.2.3"),
            IPAddress.Parse("255.255.255.255"),
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, "IPv4");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateIPv4ColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            IPAddress.Parse);

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Float32Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_float32_{Guid.NewGuid():N}";
        var values = new[] { -123.5f, 0f, 12345.125f };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Float32");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateFloat32ColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => float.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Float64Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_float64_{Guid.NewGuid():N}";
        var values = new[] { -123.456, 0.0, 789.123 };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Float64");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateFloat64ColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => double.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Decimal32Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_decimal32_{Guid.NewGuid():N}";
        const int scale = 4;
        var values = new[]
        {
            -123.4567m,
            0m,
            98765.4321m,
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, $"Decimal32({scale})");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateDecimal32ColumnWriter("Value", scale)
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => decimal.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Decimal64Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_decimal64_{Guid.NewGuid():N}";
        const int scale = 6;
        var values = new[]
        {
            -123456789.012345m,
            0m,
            987654321.987654m,
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, $"Decimal64({scale})");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateDecimal64ColumnWriter("Value", scale)
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => decimal.Parse(s, CultureInfo.InvariantCulture));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Decimal128Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_decimal128_{Guid.NewGuid():N}";
        const int scale = 10;
        var valueStrings = new[]
        {
            "-12345678901234567890.1234567890",
            "0.0000000000",
            "98765432109876543210.0987654321",
        };
        var values = valueStrings.Select(v => CreateDecimal128Value(v, scale)).ToArray();

        await _sut.CreateSingleColumnTableAsync(_tableName, $"Decimal128({scale})");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateDecimal128ColumnWriter("Value", scale)
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => CreateDecimal128Value(s, scale));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Decimal256Column_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_decimal256_{Guid.NewGuid():N}";
        const int scale = 18;
        var valueStrings = new[]
        {
            "-12345678901234567890123456789012345.123456789012345678",
            "0.000000000000000000",
            "98765432109876543210987654321098765.876543210987654321",
        };
        var values = valueStrings.Select(v => CreateDecimal256Value(v, scale)).ToArray();

        await _sut.CreateSingleColumnTableAsync(_tableName, $"Decimal256({scale})");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateDecimal256ColumnWriter("Value", scale)
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            s => CreateDecimal256Value(s, scale));

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task BoolColumn_RoundTripsNativeBlock()
    {
        _tableName = $"default.native_bool_{Guid.NewGuid():N}";
        var values = new[] { true, false, true };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Bool");

        using var writer = new NativeFormatBlockWriter(
            columnsCount: 1,
            rowsCount: values.Length);

        writer
            .CreateBoolColumnWriter("Value")
            .WriteAll(values);

        await _sut.InsertNativePayloadAsync(_tableName, writer.GetWrittenBuffer());

        var fetched = await _sut.FetchCsvColumnAsync(
            _tableName,
            bool.Parse);

        fetched
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task NativeFormatBlockWriter_WritesAllSupportedTypesTogether()
    {
        _tableName = $"default.native_all_types_{Guid.NewGuid():N}";

        var columns = new (string Name, string Type)[]
        {
            ("UInt8Value", "UInt8"),
            ("UInt16Value", "UInt16"),
            ("UInt32Value", "UInt32"),
            ("UInt64Value", "UInt64"),
            ("UInt128Value", "UInt128"),
            ("Int8Value", "Int8"),
            ("Int16Value", "Int16"),
            ("Int32Value", "Int32"),
            ("Int64Value", "Int64"),
            ("Int128Value", "Int128"),
            ("Decimal32Value", "Decimal32(4)"),
            ("Decimal64Value", "Decimal64(6)"),
            ("Decimal128Value", "Decimal128(10)"),
            ("Decimal256Value", "Decimal256(18)"),
            ("Float32Value", "Float32"),
            ("Float64Value", "Float64"),
            ("BoolValue", "Bool"),
            ("DateValue", "Date"),
            ("Date32Value", "Date32"),
            ("DateTime64Value", "DateTime64(6)"),
            ("IPv4Value", "IPv4"),
            ("StringValue", "String"),
            ("NullableStringValue", "Nullable(String)"),
            ("LowCardValue", "LowCardinality(String)"),
            ("FixedStringValue", "FixedString(8)"),
        };

        await _sut.CreateTableAsync(_tableName, columns);

        var uint8Values = new byte[] { 0, 1, 2 };
        var uint16Values = new ushort[] { 10, 100, 1000 };
        var uint32Values = new uint[] { 1, 1000, 1000000 };
        var uint64Values = new[] { 1UL, 1_000_000UL, 123_456_789_012UL };
        var uint128Values = new[]
        {
            UInt128.Zero,
            UInt128.Parse("123456789012345678901234567890", CultureInfo.InvariantCulture),
            UInt128.Parse("3402823669209384634633746074317682", CultureInfo.InvariantCulture),
        };
        var int8Values = new sbyte[] { -8, 0, 8 };
        var int16Values = new short[] { -32000, 0, 32000 };
        var int32Values = new[] { -2_000_000_000, 0, 2_000_000_000 };
        var int64Values = new[] { -9_000_000_000, 0, 9_000_000_000 };
        var int128Values = new[]
        {
            Int128.Parse("-12345678901234567890", CultureInfo.InvariantCulture),
            Int128.Zero,
            Int128.Parse("12345678901234567890", CultureInfo.InvariantCulture),
        };
        var decimal32Values = new[]
        {
            -123.4567m,
            0m,
            98765.4321m,
        };
        var decimal64Values = new[]
        {
            -123456789.012345m,
            0m,
            987654321.987654m,
        };
        var decimal128ValueStrings = new[]
        {
            "-12345678901234567890.1234567890",
            "0.0000000000",
            "98765432109876543210.0987654321",
        };
        var decimal128Values = decimal128ValueStrings.Select(v => CreateDecimal128Value(v, scale: 10)).ToArray();
        var decimal256ValueStrings = new[]
        {
            "-12345678901234567890123456789012345.123456789012345678",
            "0.000000000000000000",
            "98765432109876543210987654321098765.876543210987654321",
        };
        var decimal256Values = decimal256ValueStrings.Select(v => CreateDecimal256Value(v, scale: 18)).ToArray();
        var float32Values = new[] { -1.5f, 0f, 3.25f };
        var float64Values = new[] { -12345.6789, 0.0, 98765.4321 };
        var boolValues = new[] { false, true, false };
        var dateValues = new[]
        {
            new DateOnly(1970, 1, 1),
            new DateOnly(2000, 2, 29),
            new DateOnly(2100, 12, 31),
        };
        var date32Values = new[]
        {
            new DateOnly(1960, 1, 1),
            new DateOnly(2024, 4, 1),
            new DateOnly(2099, 12, 31),
        };
        var dateTime64Values = new[]
        {
            new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2024, 6, 1, 12, 30, 15, TimeSpan.FromHours(2)),
            new DateTimeOffset(2025, 12, 31, 23, 59, 59, TimeSpan.FromHours(-5)),
        };
        var ipv4Values = new[]
        {
            IPAddress.Parse("127.0.0.1"),
            IPAddress.Parse("10.0.0.1"),
            IPAddress.Parse("192.168.1.1"),
        };
        var stringValues = new[] { "alpha", "beta", "gamma" };
        var nullableStringValues = new[] { "nullable", null, "again" };
        var lowCardValues = new[] { "foo", "bar", "foo" };
        var fixedStringValues = new[] { "fixed", "strings", "data" };

        const int rowCount = 3;
        using var writer = new NativeFormatBlockWriter(
            columnsCount: columns.Length,
            rowsCount: rowCount);
        writer.CreateUInt8ColumnWriter("UInt8Value").WriteAll(uint8Values);
        writer.CreateUInt16ColumnWriter("UInt16Value").WriteAll(uint16Values);
        writer.CreateUInt32ColumnWriter("UInt32Value").WriteAll(uint32Values);
        writer.CreateUInt64ColumnWriter("UInt64Value").WriteAll(uint64Values);
        writer.CreateUInt128ColumnWriter("UInt128Value").WriteAll(uint128Values);
        writer.CreateInt8ColumnWriter("Int8Value").WriteAll(int8Values);
        writer.CreateInt16ColumnWriter("Int16Value").WriteAll(int16Values);
        writer.CreateInt32ColumnWriter("Int32Value").WriteAll(int32Values);
        writer.CreateInt64ColumnWriter("Int64Value").WriteAll(int64Values);
        writer.CreateInt128ColumnWriter("Int128Value").WriteAll(int128Values);
        writer.CreateDecimal32ColumnWriter("Decimal32Value", 4).WriteAll(decimal32Values);
        writer.CreateDecimal64ColumnWriter("Decimal64Value", 6).WriteAll(decimal64Values);
        writer.CreateDecimal128ColumnWriter("Decimal128Value", 10).WriteAll(decimal128Values);
        writer.CreateDecimal256ColumnWriter("Decimal256Value", 18).WriteAll(decimal256Values);
        writer.CreateFloat32ColumnWriter("Float32Value").WriteAll(float32Values);
        writer.CreateFloat64ColumnWriter("Float64Value").WriteAll(float64Values);
        writer.CreateBoolColumnWriter("BoolValue").WriteAll(boolValues);
        writer.CreateDateColumnWriter("DateValue").WriteAll(dateValues);
        writer.CreateDate32ColumnWriter("Date32Value").WriteAll(date32Values);
        writer.CreateDateTime64ColumnWriter("DateTime64Value", 6, string.Empty).WriteAll(dateTime64Values);
        writer.CreateIPv4ColumnWriter("IPv4Value").WriteAll(ipv4Values);
        writer.CreateStringColumnWriter("StringValue").WriteAll(stringValues);
        writer.CreateNullableStringColumnWriter("NullableStringValue").WriteAll(nullableStringValues);
        writer.CreateLowCardinalityStringColumnWriter("LowCardValue").WriteAll(lowCardValues);
        writer.CreateFixedStringColumnWriter("FixedStringValue", 8).WriteAll(fixedStringValues);
        await _sut.InsertNativePayloadAsync(
            tableName: _tableName,
            payload: writer.GetWrittenBuffer());

        (await _sut.FetchCsvColumnAsync(
                tableName: _tableName,
                columnExpression: "UInt8Value",
                converter: s => byte.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(uint8Values);

        (await _sut.FetchCsvColumnAsync(
                tableName: _tableName,
                columnExpression: "UInt16Value",
                converter: s => ushort.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(uint16Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "UInt32Value", s => uint.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(uint32Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "UInt64Value", s => ulong.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(uint64Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "UInt128Value", s => UInt128.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(uint128Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "Int8Value", s => sbyte.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(int8Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "Int16Value", s => short.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(int16Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "Int32Value", s => int.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(int32Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "Int64Value", s => long.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(int64Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "Int128Value", s => Int128.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(int128Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "Decimal32Value", s => decimal.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(decimal32Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "Decimal64Value", s => decimal.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(decimal64Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "Decimal128Value", s => CreateDecimal128Value(s, 10)))
            .Should()
            .Equal(decimal128Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "Decimal256Value", s => CreateDecimal256Value(s, 18)))
            .Should()
            .Equal(decimal256Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "Float32Value", s => float.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(float32Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "Float64Value", s => double.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(float64Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "BoolValue", bool.Parse))
            .Should()
            .Equal(boolValues);

        (await _sut.FetchCsvColumnAsync(_tableName, "DateValue", s => DateOnly.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(dateValues);

        (await _sut.FetchCsvColumnAsync(_tableName, "Date32Value", s => DateOnly.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(date32Values);

        var expectedDateTime64 = dateTime64Values.Select(ToUnixTimeNanoseconds).ToArray();

        (await _sut.FetchCsvColumnAsync(_tableName, "toUnixTimestamp64Nano(DateTime64Value)", s => long.Parse(s, CultureInfo.InvariantCulture)))
            .Should()
            .Equal(expectedDateTime64);

        (await _sut.FetchCsvColumnAsync(_tableName, "IPv4Value", IPAddress.Parse))
            .Should()
            .Equal(ipv4Values);

        (await _sut.FetchCsvColumnAsync(_tableName, "StringValue", static s => s))
            .Should()
            .Equal(stringValues);

        (await _sut.FetchCsvColumnAsync(_tableName, "NullableStringValue", s => s == "\\N" ? null : s))
            .Should()
            .Equal(nullableStringValues);

        (await _sut.FetchCsvColumnAsync(_tableName, "LowCardValue", static s => s))
            .Should()
            .Equal(lowCardValues);

        (await _sut.FetchCsvColumnAsync(_tableName, "FixedStringValue", s => s.TrimEnd('\0')))
            .Should()
            .Equal(fixedStringValues);
    }

    private static Decimal128Value CreateDecimal128Value(string text, int scale)
    {
        var (rawValue, fractionDigits) = ParseDecimalComponents(text);
        if (fractionDigits > scale)
        {
            throw new ArgumentException($"Value '{text}' has more fractional digits ({fractionDigits}) than the target scale {scale}.", nameof(text));
        }

        var adjusted = rawValue * BigInteger.Pow(10, scale - fractionDigits);
        var unscaledString = adjusted.ToString(CultureInfo.InvariantCulture);
        var unscaled = Int128.Parse(unscaledString, CultureInfo.InvariantCulture);
        return Decimal128Value.FromUnscaled(unscaled, scale);
    }

    private static Decimal256Value CreateDecimal256Value(string text, int scale)
    {
        var (rawValue, fractionDigits) = ParseDecimalComponents(text);
        if (fractionDigits > scale)
        {
            throw new ArgumentException($"Value '{text}' has more fractional digits ({fractionDigits}) than the target scale {scale}.", nameof(text));
        }

        var adjusted = rawValue * BigInteger.Pow(10, scale - fractionDigits);
        return Decimal256Value.FromUnscaled(adjusted, scale);
    }

    private static (BigInteger Value, int FractionDigits) ParseDecimalComponents(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            throw new ArgumentException("Decimal text must not be empty.", nameof(text));
        }

        var trimmed = text.Trim();
        var sign = 1;
        if (trimmed.StartsWith("+", StringComparison.Ordinal))
        {
            trimmed = trimmed[1..];
        }
        else if (trimmed.StartsWith("-", StringComparison.Ordinal))
        {
            sign = -1;
            trimmed = trimmed[1..];
        }

        var parts = trimmed.Split('.', 2);
        var integerPart = parts[0];
        var fractionalPart = parts.Length > 1 ? parts[1] : string.Empty;

        if (integerPart.Length == 0)
        {
            integerPart = "0";
        }

        var digits = integerPart + fractionalPart;
        if (digits.Length == 0)
        {
            digits = "0";
        }

        var value = BigInteger.Parse(digits, CultureInfo.InvariantCulture);
        if (sign < 0)
        {
            value = BigInteger.Negate(value);
        }

        return (value, fractionalPart.Length);
    }

    private static long ToUnixTimeNanoseconds(DateTimeOffset value)
    {
        var utcTicks = value.ToUniversalTime().Ticks;
        return checked((utcTicks - DateTime.UnixEpoch.Ticks) * 100L);
    }

    public async ValueTask DisposeAsync()
    {
        if (_tableName != null)
        {
            await _sut.DropTableAsync(_tableName);
        }
    }
}


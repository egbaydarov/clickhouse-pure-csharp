using System.Globalization;
using System.Linq;
using System.Net;
using System.Numerics;
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
    public async Task Int8Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_int8_{Guid.NewGuid():N}";
        var values = new sbyte[] { sbyte.MinValue, 0, sbyte.MaxValue };

        await sut.CreateSingleColumnTableAsync(tableName, "Int8");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceInt8ColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => sbyte.Parse(s, CultureInfo.InvariantCulture));

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
    public async Task Int16Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_int16_{Guid.NewGuid():N}";
        var values = new short[] { short.MinValue, -1, short.MaxValue };

        await sut.CreateSingleColumnTableAsync(tableName, "Int16");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceInt16ColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => short.Parse(s, CultureInfo.InvariantCulture));

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
    public async Task Int32Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_int32_{Guid.NewGuid():N}";
        var values = new[] { int.MinValue, 0, int.MaxValue };

        await sut.CreateSingleColumnTableAsync(tableName, "Int32");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceInt32ColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => int.Parse(s, CultureInfo.InvariantCulture));

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
    public async Task Int64Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_int64_{Guid.NewGuid():N}";
        var values = new[] { long.MinValue, -1234567890L, long.MaxValue };

        await sut.CreateSingleColumnTableAsync(tableName, "Int64");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceInt64ColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => long.Parse(s, CultureInfo.InvariantCulture));

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
    public async Task Int128Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_int128_{Guid.NewGuid():N}";
        var values = new[]
        {
            Int128.MinValue,
            Int128.Parse("-1234567890123456789012345678901234", CultureInfo.InvariantCulture),
            Int128.MaxValue,
        };

        await sut.CreateSingleColumnTableAsync(tableName, "Int128");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceInt128ColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => Int128.Parse(s, CultureInfo.InvariantCulture));

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
    public async Task UInt8Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_uint8_{Guid.NewGuid():N}";
        var values = new byte[] { 0, 1, byte.MaxValue };

        await sut.CreateSingleColumnTableAsync(tableName, "UInt8");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceUInt8ColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => byte.Parse(s, CultureInfo.InvariantCulture));

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
    public async Task UInt16Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_uint16_{Guid.NewGuid():N}";
        var values = new ushort[] { 0, 1, ushort.MaxValue };

        await sut.CreateSingleColumnTableAsync(tableName, "UInt16");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceUInt16ColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => ushort.Parse(s, CultureInfo.InvariantCulture));

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
    public async Task UInt64Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_uint64_{Guid.NewGuid():N}";
        var values = new[] { 0UL, 1UL, ulong.MaxValue };

        await sut.CreateSingleColumnTableAsync(tableName, "UInt64");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceUInt64ColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => ulong.Parse(s, CultureInfo.InvariantCulture));

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
    public async Task UInt128Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_uint128_{Guid.NewGuid():N}";
        var values = new[]
        {
            UInt128.Zero,
            UInt128.Parse("123456789012345678901234567890123456", CultureInfo.InvariantCulture),
            UInt128.MaxValue,
        };

        await sut.CreateSingleColumnTableAsync(tableName, "UInt128");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceUInt128ColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => UInt128.Parse(s, CultureInfo.InvariantCulture));

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
    public async Task NullableStringColumn_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_nullable_string_{Guid.NewGuid():N}";
        var values = new[] { null, "alpha", null, "beta" };

        await sut.CreateSingleColumnTableAsync(tableName, "Nullable(String)");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceNullableStringColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => s == "\\N" ? null : s);

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
    public async Task FixedStringColumn_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_fixed_string_{Guid.NewGuid():N}";
        var values = new[] { "short", "exact8ch", "pad" };

        await sut.CreateSingleColumnTableAsync(tableName, "FixedString(8)");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceFixedStringColumnWriter("Value", 8);
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => s.TrimEnd('\0'));

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
    public async Task LowCardinalityStringColumn_LargeDictionary_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_lowcard_large_{Guid.NewGuid():N}";
        var values = Enumerable.Range(0, 300).Select(i => $"Value_{i:D3}").ToList();

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
    public async Task DateColumn_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_date_{Guid.NewGuid():N}";
        var values = new[]
        {
            new DateOnly(1970, 1, 1),
            new DateOnly(2000, 2, 29),
            new DateOnly(2106, 2, 7),
        };

        await sut.CreateSingleColumnTableAsync(tableName, "Date");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceDateColumnWriter("Value");
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
    public async Task DateTime64Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_datetime64_{Guid.NewGuid():N}";
        var values = new[]
        {
            new DateTimeOffset(2023, 1, 1, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2024, 5, 12, 15, 30, 45, TimeSpan.FromHours(3)),
            new DateTimeOffset(2035, 12, 31, 23, 59, 59, TimeSpan.FromHours(-4)),
        };

        await sut.CreateSingleColumnTableAsync(tableName, "DateTime64(3)");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceDateTime64ColumnWriter("Value", 3, string.Empty);
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var expected = values.Select(ToUnixTimeNanoseconds).ToArray();

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                "toUnixTimestamp64Nano(Value)",
                s => long.Parse(s, CultureInfo.InvariantCulture));

            fetched
                .Should()
                .Equal(expected);
        }
        finally
        {
            await sut.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task DateTime64Column_WithTimeZone_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_datetime64_tz_{Guid.NewGuid():N}";
        const string timeZone = "Europe/Berlin";
        var values = new[]
        {
            new DateTimeOffset(2022, 3, 27, 1, 59, 59, TimeSpan.Zero),
            new DateTimeOffset(2022, 3, 27, 2, 0, 1, TimeSpan.Zero),
            new DateTimeOffset(2022, 10, 30, 1, 30, 0, TimeSpan.Zero),
        };

        await sut.CreateSingleColumnTableAsync(tableName, $"DateTime64(6, '{timeZone}')");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceDateTime64ColumnWriter("Value", 6, timeZone);
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var expected = values.Select(ToUnixTimeNanoseconds).ToArray();

            var fetchedNanos = await sut.FetchCsvColumnAsync(
                tableName,
                "toUnixTimestamp64Nano(Value)",
                s => long.Parse(s, CultureInfo.InvariantCulture));

            fetchedNanos
                .Should()
                .Equal(expected);

            var fetchedLocal = await sut.FetchCsvColumnAsync(
                tableName,
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
        finally
        {
            await sut.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task IPv4Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_ipv4_{Guid.NewGuid():N}";
        var values = new[]
        {
            IPAddress.Parse("0.0.0.0"),
            IPAddress.Parse("10.1.2.3"),
            IPAddress.Parse("255.255.255.255"),
        };

        await sut.CreateSingleColumnTableAsync(tableName, "IPv4");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceIPv4ColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                IPAddress.Parse);

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
    public async Task Float32Column_RoundTripsNativeBlock()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_float32_{Guid.NewGuid():N}";
        var values = new[] { -123.5f, 0f, 12345.125f };

        await sut.CreateSingleColumnTableAsync(tableName, "Float32");

        try
        {
            var payload = BuildPayload(values.Length, writer =>
            {
                var column = writer.AdvanceFloat32ColumnWriter("Value");
                foreach (var value in values)
                {
                    column.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            var fetched = await sut.FetchCsvColumnAsync(
                tableName,
                s => float.Parse(s, CultureInfo.InvariantCulture));

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

    [Fact]
    public async Task NativeFormatBlockWriter_WritesAllSupportedTypesTogether()
    {
        var sut = SutFactory.Create();
        var tableName = $"default.native_all_types_{Guid.NewGuid():N}";

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

        await sut.CreateTableAsync(tableName, columns);

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

        try
        {
            var payload = BuildPayload(columns.Length, rowCount, writer =>
            {
                var uint8Column = writer.AdvanceUInt8ColumnWriter("UInt8Value");
                foreach (var value in uint8Values)
                {
                    uint8Column.WriteCellValueAndAdvance(value);
                }

                var uint16Column = writer.AdvanceUInt16ColumnWriter("UInt16Value");
                foreach (var value in uint16Values)
                {
                    uint16Column.WriteCellValueAndAdvance(value);
                }

                var uint32Column = writer.AdvanceUInt32ColumnWriter("UInt32Value");
                foreach (var value in uint32Values)
                {
                    uint32Column.WriteCellValueAndAdvance(value);
                }

                var uint64Column = writer.AdvanceUInt64ColumnWriter("UInt64Value");
                foreach (var value in uint64Values)
                {
                    uint64Column.WriteCellValueAndAdvance(value);
                }

                var uint128Column = writer.AdvanceUInt128ColumnWriter("UInt128Value");
                foreach (var value in uint128Values)
                {
                    uint128Column.WriteCellValueAndAdvance(value);
                }

                var int8Column = writer.AdvanceInt8ColumnWriter("Int8Value");
                foreach (var value in int8Values)
                {
                    int8Column.WriteCellValueAndAdvance(value);
                }

                var int16Column = writer.AdvanceInt16ColumnWriter("Int16Value");
                foreach (var value in int16Values)
                {
                    int16Column.WriteCellValueAndAdvance(value);
                }

                var int32Column = writer.AdvanceInt32ColumnWriter("Int32Value");
                foreach (var value in int32Values)
                {
                    int32Column.WriteCellValueAndAdvance(value);
                }

                var int64Column = writer.AdvanceInt64ColumnWriter("Int64Value");
                foreach (var value in int64Values)
                {
                    int64Column.WriteCellValueAndAdvance(value);
                }

                var int128Column = writer.AdvanceInt128ColumnWriter("Int128Value");
                foreach (var value in int128Values)
                {
                    int128Column.WriteCellValueAndAdvance(value);
                }

                var float32Column = writer.AdvanceFloat32ColumnWriter("Float32Value");
                foreach (var value in float32Values)
                {
                    float32Column.WriteCellValueAndAdvance(value);
                }

                var float64Column = writer.AdvanceFloat64ColumnWriter("Float64Value");
                foreach (var value in float64Values)
                {
                    float64Column.WriteCellValueAndAdvance(value);
                }

                var boolColumn = writer.AdvanceBoolColumnWriter("BoolValue");
                foreach (var value in boolValues)
                {
                    boolColumn.WriteCellValueAndAdvance(value);
                }

                var dateColumn = writer.AdvanceDateColumnWriter("DateValue");
                foreach (var value in dateValues)
                {
                    dateColumn.WriteCellValueAndAdvance(value);
                }

                var date32Column = writer.AdvanceDate32ColumnWriter("Date32Value");
                foreach (var value in date32Values)
                {
                    date32Column.WriteCellValueAndAdvance(value);
                }

                var dateTime64Column = writer.AdvanceDateTime64ColumnWriter("DateTime64Value", 6, string.Empty);
                foreach (var value in dateTime64Values)
                {
                    dateTime64Column.WriteCellValueAndAdvance(value);
                }

                var ipv4Column = writer.AdvanceIPv4ColumnWriter("IPv4Value");
                foreach (var value in ipv4Values)
                {
                    ipv4Column.WriteCellValueAndAdvance(value);
                }

                var stringColumn = writer.AdvanceStringColumnWriter("StringValue");
                foreach (var value in stringValues)
                {
                    stringColumn.WriteCellValueAndAdvance(value);
                }

                var nullableColumn = writer.AdvanceNullableStringColumnWriter("NullableStringValue");
                foreach (var value in nullableStringValues)
                {
                    nullableColumn.WriteCellValueAndAdvance(value);
                }

                var lowCardColumn = writer.AdvanceLowCardinalityStringColumnWriter("LowCardValue");
                foreach (var value in lowCardValues)
                {
                    lowCardColumn.WriteCellValueAndAdvance(value);
                }
                lowCardColumn.GetColumnData();

                var fixedStringColumn = writer.AdvanceFixedStringColumnWriter("FixedStringValue", 8);
                foreach (var value in fixedStringValues)
                {
                    fixedStringColumn.WriteCellValueAndAdvance(value);
                }
            });

            await sut.InsertNativePayloadAsync(tableName, payload);

            (await sut.FetchCsvColumnAsync(tableName, "UInt8Value", s => byte.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(uint8Values);

            (await sut.FetchCsvColumnAsync(tableName, "UInt16Value", s => ushort.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(uint16Values);

            (await sut.FetchCsvColumnAsync(tableName, "UInt32Value", s => uint.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(uint32Values);

            (await sut.FetchCsvColumnAsync(tableName, "UInt64Value", s => ulong.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(uint64Values);

            (await sut.FetchCsvColumnAsync(tableName, "UInt128Value", s => UInt128.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(uint128Values);

            (await sut.FetchCsvColumnAsync(tableName, "Int8Value", s => sbyte.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(int8Values);

            (await sut.FetchCsvColumnAsync(tableName, "Int16Value", s => short.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(int16Values);

            (await sut.FetchCsvColumnAsync(tableName, "Int32Value", s => int.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(int32Values);

            (await sut.FetchCsvColumnAsync(tableName, "Int64Value", s => long.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(int64Values);

            (await sut.FetchCsvColumnAsync(tableName, "Int128Value", s => Int128.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(int128Values);

            (await sut.FetchCsvColumnAsync(tableName, "Float32Value", s => float.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(float32Values);

            (await sut.FetchCsvColumnAsync(tableName, "Float64Value", s => double.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(float64Values);

            (await sut.FetchCsvColumnAsync(tableName, "BoolValue", bool.Parse))
                .Should()
                .Equal(boolValues);

            (await sut.FetchCsvColumnAsync(tableName, "DateValue", s => DateOnly.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(dateValues);

            (await sut.FetchCsvColumnAsync(tableName, "Date32Value", s => DateOnly.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(date32Values);

            var expectedDateTime64 = dateTime64Values.Select(ToUnixTimeNanoseconds).ToArray();

            (await sut.FetchCsvColumnAsync(tableName, "toUnixTimestamp64Nano(DateTime64Value)", s => long.Parse(s, CultureInfo.InvariantCulture)))
                .Should()
                .Equal(expectedDateTime64);

            (await sut.FetchCsvColumnAsync(tableName, "IPv4Value", IPAddress.Parse))
                .Should()
                .Equal(ipv4Values);

            (await sut.FetchCsvColumnAsync(tableName, "StringValue", static s => s))
                .Should()
                .Equal(stringValues);

            (await sut.FetchCsvColumnAsync(tableName, "NullableStringValue", s => s == "\\N" ? null : s))
                .Should()
                .Equal(nullableStringValues);

            (await sut.FetchCsvColumnAsync(tableName, "LowCardValue", static s => s))
                .Should()
                .Equal(lowCardValues);

            (await sut.FetchCsvColumnAsync(tableName, "FixedStringValue", s => s.TrimEnd('\0')))
                .Should()
                .Equal(fixedStringValues);
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
        return BuildPayload(columnsCount: 1, rowCount, fill);
    }

    private static byte[] BuildPayload(
        int columnsCount,
        int rowCount,
        Action<NativeFormatBlockWriter> fill)
    {
        using var writer = new NativeFormatBlockWriter(columnsCount: (ulong)columnsCount, rowsCount: (ulong)rowCount);
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

    private static long ToUnixTimeNanoseconds(DateTimeOffset value)
    {
        var utcTicks = value.ToUniversalTime().Ticks;
        return checked((utcTicks - DateTime.UnixEpoch.Ticks) * 100L);
    }
}


using System.Globalization;
using System.Net;
using AwesomeAssertions;
using Clickhouse.Pure.Columns;
using Xunit;

namespace Clickhouse.Pure.Grpc.Tests;

public class NativeFormatBlockReaderTests : IAsyncDisposable
{
    private readonly Sut _sut = SutFactory.Create();
    private string? _tableName;

    [Fact]
    public async Task UInt32Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_uint32_{Guid.NewGuid():N}";
        var values = new[] { 0u, 1u, uint.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "UInt32");
        await _sut.InsertCsvAsync(_tableName, values.Select(v => v.ToString(CultureInfo.InvariantCulture)));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadUInt32Column();
            var result = new List<uint>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task NativeFormatBlockReader_ReadsAllSupportedTypesTogether()
    {
        _tableName = $"default.native_read_all_types_{Guid.NewGuid():N}";

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
        var columnNames = string.Join(", ", columns.Select(c => c.Name));

        var rows = Enumerable.Range(0, rowCount)
            .Select(i => CombineCsvValues(
                FormatCsvNumeric(uint8Values[i]),
                FormatCsvNumeric(uint16Values[i]),
                FormatCsvNumeric(uint32Values[i]),
                FormatCsvNumeric(uint64Values[i]),
                FormatCsvNumeric(uint128Values[i]),
                FormatCsvNumeric(int8Values[i]),
                FormatCsvNumeric(int16Values[i]),
                FormatCsvNumeric(int32Values[i]),
                FormatCsvNumeric(int64Values[i]),
                FormatCsvNumeric(int128Values[i]),
                FormatCsvFloat(float32Values[i]),
                FormatCsvDouble(float64Values[i]),
                FormatCsvBool(boolValues[i]),
                FormatCsvDate(dateValues[i]),
                FormatCsvDate(date32Values[i]),
                FormatCsvDateTime(dateTime64Values[i], 6),
                ipv4Values[i].ToString(),
                FormatCsvString(stringValues[i]),
                FormatCsvNullableString(nullableStringValues[i]),
                FormatCsvString(lowCardValues[i]),
                FormatCsvString(fixedStringValues[i])))
            .ToList();

        await _sut.InsertCsvAsync(_tableName, rows);

        using var reader = await _sut.QueryNativeBulkAsync($"SELECT {columnNames} FROM {_tableName}");

        var actualUInt8 = new List<byte>();
        var actualUInt16 = new List<ushort>();
        var actualUInt32 = new List<uint>();
        var actualUInt64 = new List<ulong>();
        var actualUInt128 = new List<UInt128>();
        var actualInt8 = new List<sbyte>();
        var actualInt16 = new List<short>();
        var actualInt32 = new List<int>();
        var actualInt64 = new List<long>();
        var actualInt128 = new List<Int128>();
        var actualFloat32 = new List<float>();
        var actualFloat64 = new List<double>();
        var actualBool = new List<bool>();
        var actualDate = new List<DateOnly>();
        var actualDate32 = new List<DateOnly>();
        var actualDateTime64 = new List<DateTimeOffset>();
        var actualIpv4 = new List<IPAddress>();
        var actualString = new List<string>();
        var actualNullableString = new List<string?>();
        var actualLowCard = new List<string>();
        var actualFixedString = new List<string>();

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

            var blockReader = response.BlockReader;

            var uint8Column = blockReader.ReadUInt8Column();
            while (uint8Column.HasMoreRows())
            {
                actualUInt8.Add(uint8Column.ReadNext());
            }

            var uint16Column = blockReader.ReadUInt16Column();
            while (uint16Column.HasMoreRows())
            {
                actualUInt16.Add(uint16Column.ReadNext());
            }

            var uint32Column = blockReader.ReadUInt32Column();
            while (uint32Column.HasMoreRows())
            {
                actualUInt32.Add(uint32Column.ReadNext());
            }

            var uint64Column = blockReader.ReadUInt64Column();
            while (uint64Column.HasMoreRows())
            {
                actualUInt64.Add(uint64Column.ReadNext());
            }

            var uint128Column = blockReader.ReadUInt128Column();
            while (uint128Column.HasMoreRows())
            {
                actualUInt128.Add(uint128Column.ReadNext());
            }

            var int8Column = blockReader.ReadInt8Column();
            while (int8Column.HasMoreRows())
            {
                actualInt8.Add(int8Column.ReadNext());
            }

            var int16Column = blockReader.ReadInt16Column();
            while (int16Column.HasMoreRows())
            {
                actualInt16.Add(int16Column.ReadNext());
            }

            var int32Column = blockReader.ReadInt32Column();
            while (int32Column.HasMoreRows())
            {
                actualInt32.Add(int32Column.ReadNext());
            }

            var int64Column = blockReader.ReadInt64Column();
            while (int64Column.HasMoreRows())
            {
                actualInt64.Add(int64Column.ReadNext());
            }

            var int128Column = blockReader.ReadInt128Column();
            while (int128Column.HasMoreRows())
            {
                actualInt128.Add(int128Column.ReadNext());
            }

            var float32Column = blockReader.ReadFloat32Column();
            while (float32Column.HasMoreRows())
            {
                actualFloat32.Add(float32Column.ReadNext());
            }

            var float64Column = blockReader.ReadFloat64Column();
            while (float64Column.HasMoreRows())
            {
                actualFloat64.Add(float64Column.ReadNext());
            }

            var boolColumn = blockReader.ReadBoolColumn();
            while (boolColumn.HasMoreRows())
            {
                actualBool.Add(boolColumn.ReadNext());
            }

            var dateColumn = blockReader.ReadDateColumn();
            while (dateColumn.HasMoreRows())
            {
                actualDate.Add(dateColumn.ReadNext());
            }

            var date32Column = blockReader.ReadDate32Column();
            while (date32Column.HasMoreRows())
            {
                actualDate32.Add(date32Column.ReadNext());
            }

            var dateTime64Column = blockReader.ReadDateTime64Column(6, string.Empty);
            while (dateTime64Column.HasMoreRows())
            {
                actualDateTime64.Add(dateTime64Column.ReadNext());
            }

            var ipv4Column = blockReader.ReadIPv4Column();
            while (ipv4Column.HasMoreRows())
            {
                actualIpv4.Add(ipv4Column.ReadNext());
            }

            var stringColumn = blockReader.ReadStringColumn();
            while (stringColumn.HasMoreRows())
            {
                actualString.Add(stringColumn.ReadNext());
            }

            var nullableStringColumn = blockReader.ReadNullableStringColumn();
            while (nullableStringColumn.HasMoreRows())
            {
                actualNullableString.Add(nullableStringColumn.ReadNext());
            }

            var lowCardColumn = blockReader.ReadLowCardinalityStringColumn();
            while (lowCardColumn.HasMoreRows())
            {
                actualLowCard.Add(lowCardColumn.ReadNext());
            }

            var fixedStringColumn = blockReader.ReadFixedStringColumn();
            while (fixedStringColumn.HasMoreRows())
            {
                actualFixedString.Add(fixedStringColumn.ReadNext());
            }
        }

        actualUInt8.Should().Equal(uint8Values);
        actualUInt16.Should().Equal(uint16Values);
        actualUInt32.Should().Equal(uint32Values);
        actualUInt64.Should().Equal(uint64Values);
        actualUInt128.Should().Equal(uint128Values);
        actualInt8.Should().Equal(int8Values);
        actualInt16.Should().Equal(int16Values);
        actualInt32.Should().Equal(int32Values);
        actualInt64.Should().Equal(int64Values);
        actualInt128.Should().Equal(int128Values);
        actualFloat32.Should().Equal(float32Values);
        actualFloat64.Should().Equal(float64Values);
        actualBool.Should().Equal(boolValues);
        actualDate.Should().Equal(dateValues);
        actualDate32.Should().Equal(date32Values);
        actualDateTime64.Select(ToUnixTimeNanoseconds).ToArray()
            .Should()
            .Equal(dateTime64Values.Select(ToUnixTimeNanoseconds).ToArray());
        actualIpv4.Should().Equal(ipv4Values);
        actualString.Should().Equal(stringValues);
        actualNullableString.Should().Equal(nullableStringValues);
        actualLowCard.Should().Equal(lowCardValues);
        actualFixedString.Should().Equal(fixedStringValues);
    }

    [Fact]
    public async Task Date32Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_date32_{Guid.NewGuid():N}";
        var values = new[]
        {
            new DateOnly(1900, 1, 1),
            new DateOnly(2024, 11, 9),
            new DateOnly(2299, 12, 31),
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Date32");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvDate));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadDate32Column();
            var result = new List<DateOnly>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task DateColumn_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_date_{Guid.NewGuid():N}";
        var values = new[]
        {
            new DateOnly(1970, 1, 1),
            new DateOnly(2000, 2, 29),
            new DateOnly(2106, 2, 7),
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Date");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvDate));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadDateColumn();
            var result = new List<DateOnly>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task DateTime64Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_datetime64_{Guid.NewGuid():N}";
        var values = new[]
        {
            new DateTimeOffset(2023, 1, 1, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2024, 5, 12, 15, 30, 45, TimeSpan.FromHours(3)),
            new DateTimeOffset(2035, 12, 31, 23, 59, 59, TimeSpan.FromHours(-4)),
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, "DateTime64(3)");
        await _sut.InsertCsvAsync(_tableName, values.Select(v => FormatCsvDateTime(v, 3)));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadDateTime64Column(3, string.Empty);
            var result = new List<DateTimeOffset>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        var expectedNanos = values.Select(ToUnixTimeNanoseconds).ToArray();
        actual.Select(ToUnixTimeNanoseconds).ToArray()
            .Should()
            .Equal(expectedNanos);
    }

    [Fact]
    public async Task DateTime64Column_WithTimeZone_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_datetime64_tz_{Guid.NewGuid():N}";
        const string timeZone = "Europe/Berlin";
        var tzInfo = TimeZoneInfo.FindSystemTimeZoneById(timeZone);

        var values = new[]
        {
            new DateTimeOffset(2022, 3, 27, 1, 59, 59, TimeSpan.Zero),
            new DateTimeOffset(2022, 3, 27, 2, 0, 1, TimeSpan.Zero),
            new DateTimeOffset(2022, 10, 30, 1, 30, 0, TimeSpan.Zero),
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, $"DateTime64(6, '{timeZone}')");
        await _sut.InsertCsvAsync(_tableName, values.Select(v => FormatCsvDateTimeWithTimezone(v, 6, tzInfo)));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadDateTime64Column(6, timeZone);
            var result = new List<DateTimeOffset>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        var expectedNanos = values.Select(ToUnixTimeNanoseconds).ToArray();
        actual.Select(ToUnixTimeNanoseconds).ToArray()
            .Should()
            .Equal(expectedNanos);
    }

    [Fact]
    public async Task IPv4Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_ipv4_{Guid.NewGuid():N}";
        var values = new[]
        {
            IPAddress.Parse("0.0.0.0"),
            IPAddress.Parse("10.1.2.3"),
            IPAddress.Parse("255.255.255.255"),
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, "IPv4");
        await _sut.InsertCsvAsync(_tableName, values.Select(v => v.ToString()));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadIPv4Column();
            var result = new List<IPAddress>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Float32Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_float32_{Guid.NewGuid():N}";
        var values = new[] { -123.5f, 0f, 12345.125f };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Float32");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvFloat));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadFloat32Column();
            var result = new List<float>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Float64Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_float64_{Guid.NewGuid():N}";
        var values = new[] { -123.456, 0.0, 789.123 };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Float64");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvDouble));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadFloat64Column();
            var result = new List<double>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task BoolColumn_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_bool_{Guid.NewGuid():N}";
        var values = new[] { true, false, true };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Bool");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvBool));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadBoolColumn();
            var result = new List<bool>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task StringColumn_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_string_{Guid.NewGuid():N}";
        var values = new[] { "hello", "world", "test" };

        await _sut.CreateSingleColumnTableAsync(_tableName, "String");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvString));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadStringColumn();
            var result = new List<string>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task NullableStringColumn_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_nullable_string_{Guid.NewGuid():N}";
        var values = new[] { null, "alpha", null, "beta" };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Nullable(String)");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvNullableString));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadNullableStringColumn();
            var result = new List<string?>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task FixedStringColumn_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_fixed_string_{Guid.NewGuid():N}";
        var values = new[] { "short", "exact8ch", "pad" };

        await _sut.CreateSingleColumnTableAsync(_tableName, "FixedString(8)");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvString));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadFixedStringColumn();
            var result = new List<string>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task LowCardinalityStringColumn_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_lowcard_{Guid.NewGuid():N}";
        var values = new[] { "alpha", "beta", "alpha", "gamma", "beta" };

        await _sut.CreateSingleColumnTableAsync(_tableName, "LowCardinality(String)");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvString));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadLowCardinalityStringColumn();
            var result = new List<string>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task LowCardinalityStringColumn_LargeDictionary_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_lowcard_large_{Guid.NewGuid():N}";
        var values = Enumerable.Range(0, 300).Select(i => $"Value_{i:D3}").ToList();

        await _sut.CreateSingleColumnTableAsync(_tableName, "LowCardinality(String)");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvString));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadLowCardinalityStringColumn();
            var result = new List<string>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task UInt8Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_uint8_{Guid.NewGuid():N}";
        var values = new byte[] { 0, 1, byte.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "UInt8");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvNumeric));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadUInt8Column();
            var result = new List<byte>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task UInt16Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_uint16_{Guid.NewGuid():N}";
        var values = new ushort[] { 0, 1, ushort.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "UInt16");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvNumeric));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadUInt16Column();
            var result = new List<ushort>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task UInt64Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_uint64_{Guid.NewGuid():N}";
        var values = new[] { 0UL, 1UL, ulong.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "UInt64");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvNumeric));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadUInt64Column();
            var result = new List<ulong>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task UInt128Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_uint128_{Guid.NewGuid():N}";
        var values = new[]
        {
            UInt128.Zero,
            UInt128.Parse("123456789012345678901234567890123456", CultureInfo.InvariantCulture),
            UInt128.MaxValue,
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, "UInt128");
        await _sut.InsertCsvAsync(_tableName, values.Select(v => v.ToString(CultureInfo.InvariantCulture)));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadUInt128Column();
            var result = new List<UInt128>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Int8Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_int8_{Guid.NewGuid():N}";
        var values = new sbyte[] { sbyte.MinValue, 0, sbyte.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Int8");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvNumeric));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadInt8Column();
            var result = new List<sbyte>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Int16Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_int16_{Guid.NewGuid():N}";
        var values = new short[] { short.MinValue, -1, short.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Int16");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvNumeric));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadInt16Column();
            var result = new List<short>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Int32Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_int32_{Guid.NewGuid():N}";
        var values = new[] { int.MinValue, 0, int.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Int32");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvNumeric));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadInt32Column();
            var result = new List<int>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Int64Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_int64_{Guid.NewGuid():N}";
        var values = new[] { long.MinValue, -1234567890L, long.MaxValue };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Int64");
        await _sut.InsertCsvAsync(_tableName, values.Select(FormatCsvNumeric));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadInt64Column();
            var result = new List<long>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    [Fact]
    public async Task Int128Column_ReadsNativeBlock()
    {
        _tableName = $"default.native_read_int128_{Guid.NewGuid():N}";
        var values = new[]
        {
            Int128.MinValue,
            Int128.Parse("-1234567890123456789012345678901234", CultureInfo.InvariantCulture),
            Int128.MaxValue,
        };

        await _sut.CreateSingleColumnTableAsync(_tableName, "Int128");
        await _sut.InsertCsvAsync(_tableName, values.Select(v => v.ToString(CultureInfo.InvariantCulture)));

        var actual = await ReadColumnAsync($"SELECT Value FROM {_tableName}", static reader =>
        {
            var column = reader.ReadInt128Column();
            var result = new List<Int128>(column.Length);
            while (column.HasMoreRows())
            {
                result.Add(column.ReadNext());
            }

            return result;
        });

        actual
            .Should()
            .Equal(values);
    }

    private async Task<List<T>> ReadColumnAsync<T>(
        string query,
        Func<NativeFormatBlockReader, List<T>> readBlock)
    {
        using var reader = await _sut.QueryNativeBulkAsync(query);
        var values = new List<T>();

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

            var blockValues = readBlock(response.BlockReader);
            values.AddRange(blockValues);
        }

        return values;
    }

    private static string FormatCsvString(string value)
    {
        return $"\"{value.Replace("\"", "\"\"")}\"";
    }

    private static string FormatCsvNullableString(string? value)
    {
        return value == null ? "\\N" : FormatCsvString(value);
    }

    private static string FormatCsvBool(bool value)
    {
        return value ? "true" : "false";
    }

    private static string FormatCsvNumeric<T>(T value)
        where T : IFormattable
    {
        return value.ToString(null, CultureInfo.InvariantCulture);
    }

    private static string FormatCsvFloat(float value)
    {
        return value.ToString("R", CultureInfo.InvariantCulture);
    }

    private static string FormatCsvDouble(double value)
    {
        return value.ToString("R", CultureInfo.InvariantCulture);
    }

    private static string FormatCsvDate(DateOnly value)
    {
        return value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
    }

    private static string FormatCsvDateTime(DateTimeOffset value, int scale)
    {
        var format = GetDateTimeFormat(scale);
        return value.ToUniversalTime().ToString(format, CultureInfo.InvariantCulture);
    }

    private static string FormatCsvDateTimeWithTimezone(DateTimeOffset value, int scale, TimeZoneInfo timeZone)
    {
        var format = GetDateTimeFormat(scale);
        var local = TimeZoneInfo.ConvertTime(value, timeZone);
        return local.DateTime.ToString(format, CultureInfo.InvariantCulture);
    }

    private static string GetDateTimeFormat(int scale)
    {
        return scale switch
        {
            0 => "yyyy-MM-dd HH:mm:ss",
            _ => $"yyyy-MM-dd HH:mm:ss.{new string('f', scale)}"
        };
    }

    private static string CombineCsvValues(params string[] values)
    {
        return string.Join(",", values);
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



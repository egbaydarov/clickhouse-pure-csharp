namespace Clickhouse.Pure.ColumnCodeGenerator;

using System;
using System.Collections.Generic;

public record class NumericColumnVariant(
    string? CsharpType,
    string? ClickhouseType,
    int ValueSizeInBytes,
    string? SpanInterpretFunction,
    IReadOnlyList<string>? WriterValueStatements)
{
    public NumericColumnVariant() : this("", "", 0, "", Array.Empty<string>())
    {
    }
}

public record class DecimalColumnVariant(
    string ClickhouseType,
    string ManagedType,
    int ValueSizeInBytes,
    int StorageBits,
    int MinPrecision,
    int MaxPrecision,
    bool ReturnsDecimal,
    bool UsesBigInteger)
{
    public DecimalColumnVariant() : this("", "", 0, 0, 0, 0, false, false)
    {
    }
}

public record class NullableColumnVariant(
    string? CsharpType,
    string? ClickhouseType)
{
    public NullableColumnVariant() : this("", "")
    {
    }
}

public record class LowCardinalityColumnVariant(
    string? CsharpType,
    string? ClickhouseType,
    bool IsNullable,
    string? Suffix)
{
    public LowCardinalityColumnVariant() : this("", "", false, "")
    {
    }
}

public record class ArrayNullableColumnVariant(
    string ClickhouseType,
    string InnerCsharpType,
    string Suffix,
    bool IsVariableLength,
    int ValueSizeInBytes,
    string? SpanReadFunction,
    string DefaultNullValue,
    string? WriteStatement)
{
    public ArrayNullableColumnVariant() : this("", "", "", false, 0, null, "", null)
    {
    }
}

public record class FixedStringColumnVariant(
    int Size)
{
    public FixedStringColumnVariant() : this(0)
    {
    }
}

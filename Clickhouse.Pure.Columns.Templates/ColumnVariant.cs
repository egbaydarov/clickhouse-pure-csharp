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
    string? ClickhouseType)
{
    public LowCardinalityColumnVariant() : this("", "")
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

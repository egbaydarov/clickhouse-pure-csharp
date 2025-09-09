namespace Clickhouse.Pure.ColumnCodeGenerator;

public record class NumericColumnVariant(
    string? CsharpType,
    string? ClickhouseType,
    int ValueSizeInBytes,
    string? SpanInterpretFunction)
{
    public NumericColumnVariant() : this("", "", 0, "")
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

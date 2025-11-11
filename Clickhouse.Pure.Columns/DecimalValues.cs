using System;
using System.Globalization;
using System.Numerics;

namespace Clickhouse.Pure.Columns;

public readonly record struct Decimal128Value(Int128 UnscaledValue, int Scale)
{
    public Decimal128Value WithScale(int targetScale)
    {
        if (targetScale == Scale)
        {
            return this;
        }

        var adjusted = DecimalMath.ChangeScale((BigInteger)UnscaledValue, Scale, targetScale);
        return new Decimal128Value(DecimalMath.BigIntegerToInt128(adjusted), targetScale);
    }

    public BigInteger ToBigInteger() => (BigInteger)UnscaledValue;

    public bool TryToDecimal(out decimal value)
    {
        value = 0;
        if ((uint)Scale >= 29u)
        {
            return false;
        }

        try
        {
            value = (decimal)UnscaledValue / DecimalMath.GetDecimalPow10(Scale);
            return true;
        }
        catch (OverflowException)
        {
            return false;
        }
    }

    public override string ToString()
    {
        return DecimalFormatting.Format(ToBigInteger(), Scale);
    }

    public static Decimal128Value FromUnscaled(Int128 value, int scale) => new(value, scale);

    public static Decimal128Value FromBigInteger(BigInteger value, int scale) => new(DecimalMath.BigIntegerToInt128(value), scale);
}

public readonly record struct Decimal256Value(BigInteger UnscaledValue, int Scale)
{
    public Decimal256Value WithScale(int targetScale)
    {
        if (targetScale == Scale)
        {
            return this;
        }

        var adjusted = DecimalMath.ChangeScale(UnscaledValue, Scale, targetScale);
        return new Decimal256Value(adjusted, targetScale);
    }

    public BigInteger ToBigInteger() => UnscaledValue;

    public bool TryToDecimal(out decimal value)
    {
        value = default;
        if ((uint)Scale >= 29u)
        {
            return false;
        }

        try
        {
            value = (decimal)UnscaledValue / DecimalMath.GetDecimalPow10(Scale);
            return true;
        }
        catch (OverflowException)
        {
            return false;
        }
    }

    public override string ToString()
    {
        return DecimalFormatting.Format(UnscaledValue, Scale);
    }

    public static Decimal256Value FromUnscaled(BigInteger value, int scale) => new(value, scale);
}

internal static class DecimalFormatting
{
    internal static string Format(BigInteger unscaledValue, int scale)
    {
        var sign = unscaledValue.Sign < 0 ? "-" : string.Empty;
        var abs = BigInteger.Abs(unscaledValue);

        var digits = abs.ToString(CultureInfo.InvariantCulture);
        if (scale == 0)
        {
            return sign + digits;
        }

        if (digits.Length <= scale)
        {
            var padded = digits.PadLeft(scale + 1, '0');
            var integerPart = padded[..(padded.Length - scale)];
            var fractionalPart = padded[(padded.Length - scale)..];
            return sign + integerPart + "." + fractionalPart;
        }
        else
        {
            var integerPart = digits[..(digits.Length - scale)];
            var fractionalPart = digits[(digits.Length - scale)..];
            return sign + integerPart + "." + fractionalPart;
        }
    }
}


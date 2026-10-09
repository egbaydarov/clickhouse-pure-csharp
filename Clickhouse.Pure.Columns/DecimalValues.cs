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

    /// <summary>
    /// Parses a decimal string (e.g. "123.45", "-0.5", "1E-7", "6802") into a <see cref="Decimal128Value"/>
    /// with the requested <paramref name="scale"/>. Extra fractional digits are rounded half away from zero.
    /// </summary>
    public static Decimal128Value Parse(string value, int scale)
    {
        if (!TryParse(value, scale, out var result))
        {
            throw new FormatException($"'{value}' is not a valid decimal string.");
        }

        return result;
    }

    /// <summary>
    /// Tries to parse a decimal string (e.g. "123.45", "-0.5", "1E-7", "6802") into a <see cref="Decimal128Value"/>
    /// with the requested <paramref name="scale"/>. Extra fractional digits are rounded half away from zero.
    /// </summary>
    public static bool TryParse(string? value, int scale, out Decimal128Value result)
    {
        result = default;
        if ((uint)scale > 38u || !DecimalParsing.TryParseUnscaled(value, out var unscaled, out var sourceScale))
        {
            return false;
        }

        var rescaled = DecimalParsing.Rescale(unscaled, sourceScale, scale);
        if (rescaled < (BigInteger)Int128.MinValue || rescaled > (BigInteger)Int128.MaxValue)
        {
            return false;
        }

        result = new Decimal128Value((Int128)rescaled, scale);
        return true;
    }
}

internal static class DecimalParsing
{
    /// <summary>
    /// Parses "[-+]digits[.digits][(e|E)[-+]digits]" into an unscaled integer and its scale.
    /// </summary>
    internal static bool TryParseUnscaled(string? value, out BigInteger unscaled, out int scale)
    {
        unscaled = BigInteger.Zero;
        scale = 0;

        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var span = value.AsSpan().Trim();

        var exponent = 0;
        var expIndex = span.IndexOfAny('e', 'E');
        if (expIndex >= 0)
        {
            if (!int.TryParse(span[(expIndex + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out exponent))
            {
                return false;
            }

            span = span[..expIndex];
        }

        var negative = false;
        if (span.Length > 0 && (span[0] == '-' || span[0] == '+'))
        {
            negative = span[0] == '-';
            span = span[1..];
        }

        var dotIndex = span.IndexOf('.');
        ReadOnlySpan<char> integerPart;
        ReadOnlySpan<char> fractionalPart;
        if (dotIndex >= 0)
        {
            integerPart = span[..dotIndex];
            fractionalPart = span[(dotIndex + 1)..];
        }
        else
        {
            integerPart = span;
            fractionalPart = ReadOnlySpan<char>.Empty;
        }

        if (integerPart.Length == 0 && fractionalPart.Length == 0)
        {
            return false;
        }

        foreach (var c in integerPart)
        {
            if (!char.IsAsciiDigit(c)) return false;
        }

        foreach (var c in fractionalPart)
        {
            if (!char.IsAsciiDigit(c)) return false;
        }

        var digits = string.Concat(integerPart, fractionalPart);
        unscaled = digits.Length == 0
            ? BigInteger.Zero
            : BigInteger.Parse(digits, NumberStyles.None, CultureInfo.InvariantCulture);

        if (negative)
        {
            unscaled = -unscaled;
        }

        scale = fractionalPart.Length - exponent;
        return true;
    }

    /// <summary>
    /// Changes the scale of an unscaled integer, rounding half away from zero when reducing scale.
    /// </summary>
    internal static BigInteger Rescale(BigInteger unscaled, int currentScale, int targetScale)
    {
        if (targetScale == currentScale || unscaled.IsZero)
        {
            return unscaled;
        }

        if (targetScale > currentScale)
        {
            return unscaled * BigInteger.Pow(10, targetScale - currentScale);
        }

        var divisor = BigInteger.Pow(10, currentScale - targetScale);
        var abs = BigInteger.Abs(unscaled);
        var quotient = BigInteger.DivRem(abs, divisor, out var remainder);
        if (remainder * 2 >= divisor)
        {
            quotient += BigInteger.One;
        }

        return unscaled.Sign < 0 ? -quotient : quotient;
    }
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


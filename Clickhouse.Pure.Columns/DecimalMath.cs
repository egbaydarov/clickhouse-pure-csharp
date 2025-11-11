#nullable enable
using System;
using System.Globalization;
using System.Numerics;

namespace Clickhouse.Pure.Columns;

internal static class DecimalMath
{
    private static readonly decimal[] Pow10Decimal;
    private static readonly BigInteger[] Pow10BigInteger;
    private static readonly BigInteger Int256Min;
    private static readonly BigInteger Int256Max;

    static DecimalMath()
    {
        Pow10Decimal = new decimal[29];
        Pow10Decimal[0] = 1m;
        for (var i = 1; i < Pow10Decimal.Length; i++)
        {
            Pow10Decimal[i] = Pow10Decimal[i - 1] * 10m;
        }

        Pow10BigInteger = new BigInteger[77];
        Pow10BigInteger[0] = BigInteger.One;
        for (var i = 1; i < Pow10BigInteger.Length; i++)
        {
            Pow10BigInteger[i] = Pow10BigInteger[i - 1] * 10;
        }

        Int256Min = -(BigInteger.One << 255);
        Int256Max = (BigInteger.One << 255) - BigInteger.One;
    }

    internal static decimal FromInt32(int value, int scale)
    {
        return value / GetDecimalPow10(scale);
    }

    internal static decimal FromInt64(long value, int scale)
    {
        return value / GetDecimalPow10(scale);
    }

    internal static int ScaleDecimalToInt32(decimal value, int scale, int precision)
    {
        var pow = GetDecimalPow10(scale);
        var scaled = value * pow;

        if (decimal.Truncate(scaled) != scaled)
        {
            throw new InvalidOperationException($"Value {value} exceeds the allowed scale {scale}.");
        }

        var intValue = decimal.ToInt32(scaled);
        EnsureFitsPrecision(intValue, precision);
        return intValue;
    }

    internal static long ScaleDecimalToInt64(decimal value, int scale, int precision)
    {
        var pow = GetDecimalPow10(scale);
        var scaled = value * pow;

        if (decimal.Truncate(scaled) != scaled)
        {
            throw new InvalidOperationException($"Value {value} exceeds the allowed scale {scale}.");
        }

        var longValue = decimal.ToInt64(scaled);
        EnsureFitsPrecision(longValue, precision);
        return longValue;
    }

    internal static decimal GetDecimalPow10(int scale)
    {
        if ((uint)scale >= Pow10Decimal.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(scale), $"Scale {scale} is outside supported range 0..{Pow10Decimal.Length - 1}.");
        }

        return Pow10Decimal[scale];
    }

    internal static BigInteger GetBigIntegerPow10(int scale)
    {
        if ((uint)scale >= Pow10BigInteger.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(scale), $"Scale {scale} is outside supported range 0..{Pow10BigInteger.Length - 1}.");
        }

        return Pow10BigInteger[scale];
    }

    internal static BigInteger ReadInt256LittleEndian(ReadOnlySpan<byte> source)
    {
        if (source.Length != 32)
        {
            throw new ArgumentOutOfRangeException(nameof(source), "Decimal256 requires exactly 32 bytes.");
        }

        return new BigInteger(source, isUnsigned: false, isBigEndian: false);
    }

    internal static void WriteInt256LittleEndian(Span<byte> destination, BigInteger value)
    {
        if (destination.Length != 32)
        {
            throw new ArgumentOutOfRangeException(nameof(destination), "Decimal256 requires exactly 32 bytes.");
        }

        if (value < Int256Min || value > Int256Max)
        {
            throw new OverflowException("Decimal256 value exceeds 256-bit range.");
        }

        var signFill = value.Sign < 0 ? (byte)0xFF : (byte)0x00;
        destination.Fill(signFill);

        if (!value.TryWriteBytes(destination, out _, isUnsigned: false, isBigEndian: false))
        {
            throw new OverflowException("Failed to serialize Decimal256 value.");
        }
    }

    internal static void EnsureFitsPrecision(int value, int precision) => EnsureFitsPrecision((long)value, precision);

    internal static void EnsureFitsPrecision(long value, int precision)
    {
        var digits = CountDigits(value);
        if (digits > precision)
        {
            throw new OverflowException($"Decimal value with {digits} digits exceeds declared precision {precision}.");
        }
    }

    internal static void EnsureFitsPrecision(Int128 value, int precision)
    {
        var digits = CountDigits(value);
        if (digits > precision)
        {
            throw new OverflowException($"Decimal value with {digits} digits exceeds declared precision {precision}.");
        }
    }

    internal static void EnsureFitsPrecision(BigInteger value, int precision)
    {
        var digits = CountDigits(value);
        if (digits > precision)
        {
            throw new OverflowException($"Decimal value with {digits} digits exceeds declared precision {precision}.");
        }
    }

    internal static int CountDigits(long value)
    {
        ulong abs;
        if (value < 0)
        {
            abs = (ulong)(-(value + 1)) + 1;
        }
        else
        {
            abs = (ulong)value;
        }

        var digits = 0;
        do
        {
            digits++;
            abs /= 10;
        }
        while (abs != 0);

        return digits;
    }

    internal static int CountDigits(Int128 value)
    {
        UInt128 abs;
        if (value < 0)
        {
            abs = (UInt128)(-value);
        }
        else
        {
            abs = (UInt128)value;
        }

        var digits = 0;
        do
        {
            digits++;
            abs /= 10;
        }
        while (abs != 0);

        return digits;
    }

    internal static int CountDigits(BigInteger value)
    {
        var abs = BigInteger.Abs(value);
        if (abs.IsZero)
        {
            return 1;
        }

        return abs.ToString(CultureInfo.InvariantCulture).Length;
    }

    internal static Int128 BigIntegerToInt128(BigInteger value)
    {
        if (value < (BigInteger)Int128.MinValue || value > (BigInteger)Int128.MaxValue)
        {
            throw new OverflowException("Value does not fit into Int128.");
        }

        return (Int128)value;
    }

    internal static BigInteger ChangeScale(BigInteger value, int currentScale, int targetScale)
    {
        if (targetScale == currentScale)
        {
            return value;
        }

        if (targetScale > currentScale)
        {
            var factor = GetBigIntegerPow10(targetScale - currentScale);
            return value * factor;
        }

        var divisor = GetBigIntegerPow10(currentScale - targetScale);
        if (!value.IsZero)
        {
            var (quotient, remainder) = BigInteger.DivRem(value, divisor);
            if (remainder != BigInteger.Zero)
            {
                throw new InvalidOperationException("Cannot reduce decimal scale without losing precision.");
            }
            return quotient;
        }

        return BigInteger.Zero;
    }
}


using System;
using System.Globalization;
using System.Text;

namespace Clickhouse.Pure.Columns;

internal readonly record struct DecimalTypeDescriptor(int Precision, int Scale, int StorageBits, string RawTypeName);

internal static class DecimalTypeParser
{
    private static readonly Encoding Utf8 = Encoding.UTF8;

    internal static DecimalTypeDescriptor Parse(ReadOnlySpan<byte> typeNameBytes, ReadOnlySpan<byte> columnNameBytes, int expectedStorageBits)
    {
        var typeName = Utf8.GetString(typeNameBytes);
        if (!TryParse(typeName, out var descriptor))
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected a Decimal type for column '{Utf8.GetString(columnNameBytes)}', but got '{typeName}'.");
        }

        if (descriptor.StorageBits != expectedStorageBits)
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected a {expectedStorageBits}-bit Decimal for column '{Utf8.GetString(columnNameBytes)}', but got '{descriptor.RawTypeName}'.");
        }

        return descriptor;
    }

    internal static void ValidatePrecision(string columnName, string typeName, int precision, int minPrecision, int maxPrecision)
    {
        if (precision < minPrecision || precision > maxPrecision)
        {
            throw new ArgumentOutOfRangeException(nameof(precision),
                $"Precision {precision} is not valid for {typeName} in column '{columnName}'. Expected range is {minPrecision}..{maxPrecision}.");
        }
    }

    internal static void ValidateScaleForPrecision(string columnName, string typeName, int scale, int precision)
    {
        if (scale < 0 || scale > precision)
        {
            throw new ArgumentOutOfRangeException(nameof(scale),
                $"Scale {scale} is not valid for {typeName} in column '{columnName}'. Scale must be in range 0..{precision}.");
        }
    }

    private static int MapPrecisionToStorageBits(int precision)
    {
        if (precision < 1 || precision > 76)
        {
            throw new ArgumentOutOfRangeException(nameof(precision), "Precision must be in range 1..76 for ClickHouse Decimal types.");
        }

        return precision switch
        {
            <= 9 => 32,
            <= 18 => 64,
            <= 38 => 128,
            _ => 256
        };
    }

    private static bool TryParse(string typeName, out DecimalTypeDescriptor descriptor)
    {
        var trimmed = typeName.Trim();

        if (trimmed.StartsWith("Decimal32(", StringComparison.OrdinalIgnoreCase))
        {
            if (!TryParseSingleArgument(trimmed, "Decimal32(", out var scale))
            {
                descriptor = default;
                return false;
            }

            if ((uint)scale > 9u)
            {
                descriptor = default;
                return false;
            }

            descriptor = new DecimalTypeDescriptor(9, scale, 32, typeName);
            return true;
        }

        if (trimmed.StartsWith("Decimal64(", StringComparison.OrdinalIgnoreCase))
        {
            if (!TryParseSingleArgument(trimmed, "Decimal64(", out var scale))
            {
                descriptor = default;
                return false;
            }

            if ((uint)scale > 18u)
            {
                descriptor = default;
                return false;
            }

            descriptor = new DecimalTypeDescriptor(18, scale, 64, typeName);
            return true;
        }

        if (trimmed.StartsWith("Decimal128(", StringComparison.OrdinalIgnoreCase))
        {
            if (!TryParseSingleArgument(trimmed, "Decimal128(", out var scale))
            {
                descriptor = default;
                return false;
            }

            if (scale < 0 || scale > 38)
            {
                descriptor = default;
                return false;
            }

            descriptor = new DecimalTypeDescriptor(38, scale, 128, typeName);
            return true;
        }

        if (trimmed.StartsWith("Decimal256(", StringComparison.OrdinalIgnoreCase))
        {
            if (!TryParseSingleArgument(trimmed, "Decimal256(", out var scale))
            {
                descriptor = default;
                return false;
            }

            if (scale < 0 || scale > 76)
            {
                descriptor = default;
                return false;
            }

            descriptor = new DecimalTypeDescriptor(76, scale, 256, typeName);
            return true;
        }

        if (trimmed.StartsWith("Decimal(", StringComparison.OrdinalIgnoreCase))
        {
            if (!TryParseArguments(trimmed, "Decimal(", 2, out var args))
            {
                descriptor = default;
                return false;
            }

            var precision = args[0];
            var scale = args[1];
            var storage = MapPrecisionToStorageBits(precision);

            descriptor = new DecimalTypeDescriptor(precision, scale, storage, typeName);
            return true;
        }

        descriptor = default;
        return false;
    }

    private static bool TryParseSingleArgument(string typeName, string prefix, out int result)
    {
        if (!TryExtractArgumentsSubstring(typeName, prefix, out var args))
        {
            result = 0;
            return false;
        }

        result = 0;
        var trimmed = args.Trim();
        if (!int.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
        {
            return false;
        }

        result = value;
        return true;
    }

    private static bool TryParseArguments(string typeName, string prefix, int expectedCount, out int[] results)
    {
        results = [];
        if (!TryExtractArgumentsSubstring(typeName, prefix, out var args))
        {
            return false;
        }

        var parts = args.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != expectedCount)
        {
            return false;
        }

        var values = new int[expectedCount];
        for (var i = 0; i < expectedCount; i++)
        {
            if (!int.TryParse(parts[i], NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
            {
                return false;
            }

            values[i] = parsed;
        }

        if (values[1] < 0 || values[1] > values[0])
        {
            return false;
        }

        results = values;
        return true;
    }

    private static bool TryExtractArgumentsSubstring(string typeName, string prefix, out string arguments)
    {
        if (!typeName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            arguments = string.Empty;
            return false;
        }

        if (!typeName.EndsWith(')'))
        {
            arguments = string.Empty;
            return false;
        }

        arguments = typeName.Substring(prefix.Length, typeName.Length - prefix.Length - 1);
        return true;
    }
}


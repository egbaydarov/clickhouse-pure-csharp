#pragma warning disable CS0414 // Field is assigned but its value is never used
using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Clickhouse.Pure.ColumnCodeGenerator;

public interface ISequentialColumnReader<out T>
{
    int Length { get; }
    bool HasMoreRows();
    T GetCellValueAndAdvance();
}

/// <summary>
/// Reads ClickHouse Native format result that consists of one or more blocks.
/// This reader focuses on reading a single block of data and exposing typed column iterators.
/// Only a subset of types used is implemented.
/// </summary>
public partial class NativeFormatBlockReader
{
    private const int MaxVarintLen64 = 10;
    private readonly ReadOnlyMemory<byte> _buffer;
    private int _offset;
    private static readonly long[] Pow10 =
    [
        1L, 10L, 100L, 1_000L, 10_000L, 100_000L, 1_000_000L,
        10_000_000L, 100_000_000L, 1_000_000_000L
    ];

    private ulong _columnsCount;
    private ulong _rowsCount;
    private ulong _columnsRead;

    public NativeFormatBlockReader(
        ReadOnlyMemory<byte> bytes)
    {
        _buffer = bytes;
        _offset = 0;

        // Native format consists of blocks. We parse the first block header now.
        ReadBlockHeader();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ReadBlockHeader()
    {
        // Some servers omit BlockInfo when it is empty and start directly with columns count.
        // Detect and handle both cases.
        _columnsCount = ReadUVarInt();
        _rowsCount = ReadUVarInt();
        _columnsRead = 0;
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ulong ReadUVarInt()
    {
        ulong x = 0;
        var s = 0;
        var span = _buffer.Span;

        for (var i = 0; i < MaxVarintLen64; i++)
        {
            if (_offset >= _buffer.Length)
            {
                throw new InvalidOperationException("Unexpected end of data while reading varint");
            }
            var b = span[_offset++];
            if (b < 0x80)
            {
                if (i == MaxVarintLen64 - 1 && b > 1)
                    throw new OverflowException("varint overflows a 64-bit integer");
                return x | ((ulong)b << s);
            }
            x |= ((ulong)(b & 0x7F)) << s;
            s += 7;
        }

        throw new OverflowException("varint too long");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ReadUVarInt(ref int offset, ReadOnlySpan<byte> dataSpan)
    {
        ulong result = 0;
        var shift = 0;
        while (true)
        {
            if (offset >= dataSpan.Length) throw new IndexOutOfRangeException("varint: read past end");
            var b = dataSpan[offset++];
            result |= ((ulong)(b & 0x7Fu)) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 63) throw new FormatException("varint too long");
        }
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ReadInt64Le(ref int offset, ReadOnlySpan<byte> dataSpan)
    {
        if (offset + 8 > dataSpan.Length) throw new IndexOutOfRangeException("int64: read past end");
        var v = (long)BinaryPrimitives.ReadUInt64LittleEndian(dataSpan.Slice(offset, 8));
        offset += 8;
        return v;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int ReadStringLength()
    {
        var len = ReadUVarInt();
        if (len > int.MaxValue) throw new OverflowException("string too long");
        return (int)len;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ReadOnlySpan<byte> ReadHeaderString()
    {
        var len = ReadStringLength();
        if (_offset + len > _buffer.Length)
        {
            throw new IndexOutOfRangeException("string header out of range");
        }
        _offset += len;

        return _buffer.Span.Slice(_offset, len);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool MatchesType(
        ReadOnlySpan<byte> actual,
        ReadOnlySpan<byte> expected)
    {
        return actual.SequenceEqual(expected);
    }
}

public static class DateOnlyExt
{
    public static DateOnly From1900_01_01Days(
        ReadOnlySpan<byte> span)
    {
        var daysSince = MemoryMarshal.Read<int>(span);
        return DateOnly.FromDayNumber(693610 + daysSince);
    }

    public static DateOnly From1970_01_01Days(
        ReadOnlySpan<byte> span)
    {
        var daysSince = MemoryMarshal.Read<ushort>(span);
        return DateOnly.FromDayNumber(719165 + daysSince);
    }
}
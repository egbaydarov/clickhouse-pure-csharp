#pragma warning disable CS0414 // Field is assigned but its value is never used
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Net;
using System.Runtime.InteropServices.ComTypes;

namespace Clickhouse.Pure.Columns;

public interface ISequentialColumnReader<out T>
{
    int Length { get; }
    bool HasMoreRows();
    T ReadNext();
}

public interface ISequentialColumnWriter<in T, out TWriter> where TWriter : allows ref struct
{
    TWriter WriteNext(T value);
    NativeFormatBlockWriter WriteAll(IEnumerable<T> values);
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
        1L,
        10L,
        100L,
        1_000L,
        10_000L,
        100_000L,
        1_000_000L,
        10_000_000L,
        100_000_000L,
        1_000_000_000L,
        10_000_000_000L,
        100_000_000_000L,
        1_000_000_000_000L,
        10_000_000_000_000L,
        100_000_000_000_000L,
        1_000_000_000_000_000L,
        10_000_000_000_000_000L,
        100_000_000_000_000_000L,
        1_000_000_000_000_000_000L
    ];

    private readonly ulong _columnsCount;
    private readonly ulong _rowsCount;
    private ulong _columnsRead;
    
    public ulong RowsCount => _rowsCount;
    public ulong ColumnsCount => _columnsCount;

    public NativeFormatBlockReader(
        ReadOnlyMemory<byte> bytes)
    {
        _buffer = bytes;
        _offset = 0;

        // Native format consists of blocks. We parse the first block header now.
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
        var start = _offset;
        _offset += len;

        return _buffer.Span.Slice(start, len);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool MatchesType(
        ReadOnlySpan<byte> actual,
        ReadOnlySpan<byte> expected)
    {
        return actual.SequenceEqual(expected);
    }
}

public partial class NativeFormatBlockWriter : IDisposable
{
    private const int MaxVarintLen64 = 10;
    private static readonly long[] Pow10 =
    [
        1L,
        10L,
        100L,
        1_000L,
        10_000L,
        100_000L,
        1_000_000L,
        10_000_000L,
        100_000_000L,
        1_000_000_000L,
        10_000_000_000L,
        100_000_000_000L,
        1_000_000_000_000L,
        10_000_000_000_000L,
        100_000_000_000_000L,
        1_000_000_000_000_000L,
        10_000_000_000_000_000L,
        100_000_000_000_000_000L,
        1_000_000_000_000_000_000L
    ];

    private readonly ulong _columnsCount;
    private readonly ulong _rowsCount;
    
    private readonly byte[] _blockHeader;
    private readonly int _blockHeaderLength;

    private ulong _columnsHeadersWritten;
    private readonly byte[][] _headers;
    private readonly int[] _headersLength;

    private readonly byte[][] _data;
    private readonly int[] _dataLength;

    public NativeFormatBlockWriter(
        int columnsCount,
        int rowsCount)
    {
        if (columnsCount < 1 || rowsCount < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(columnsCount));
        }

        _columnsCount = (ulong)columnsCount;
        _rowsCount = (ulong)rowsCount;
        _columnsHeadersWritten = 0;

        _headers = new byte[_columnsCount][];
        _headersLength = new int[_columnsCount];
        _data = new byte[_columnsCount][];
        _dataLength = new int[_columnsCount];
        
        _blockHeader = new byte[MaxVarintLen64 * 2];

        var buffer = ArrayPool<byte>.Shared.Rent(MaxVarintLen64 * 2);
        var offset = 0;
        
        offset += WriteUVarInt(buffer.AsSpan(offset), _columnsCount);
        offset += WriteUVarInt(buffer.AsSpan(offset), _rowsCount);

        _blockHeaderLength = offset;
        _blockHeader = buffer;
    }

    public void Dispose()
    {
        ReturnPooledSegments();
        GC.SuppressFinalize(this);
    }

    ~NativeFormatBlockWriter()
    {
        ReturnPooledSegments();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int WriteUVarInt(Span<byte> destination, ulong value)
    {
        var offset = 0;
        while (value >= 0x80)
        {
            destination[offset++] = (byte)((uint)value | 0x80);
            value >>= 7;
        }
        destination[offset++] = (byte)value;
        return offset;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteInt64Le(Span<byte> destination, long value)
    {
        BinaryPrimitives.WriteInt64LittleEndian(destination, value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int WriteHeaderString(Span<byte> destination, string str)
    {
        var byteCount = Encoding.UTF8.GetByteCount(str);
        var offset = WriteUVarInt(destination, (ulong)byteCount);
        offset += Encoding.UTF8.GetBytes(str.AsSpan(), destination.Slice(offset));
        return offset;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ulong WriteColumnHeader(
        byte[] buffer,
        string columnName,
        string typeName,
        int dataLength)
    {
        if (_columnsHeadersWritten >= _columnsCount)
        {
            throw new InvalidOperationException("All declared columns have already been written.");
        }

        var nameByteCount = Encoding.UTF8.GetByteCount(columnName);
        var typeByteCount = Encoding.UTF8.GetByteCount(typeName);
        var headerBuffer = ArrayPool<byte>.Shared.Rent(MaxVarintLen64 * 2 + nameByteCount + typeByteCount);
        
        var offset = 0;
        offset += WriteHeaderString(headerBuffer.AsSpan(offset), columnName);
        offset += WriteHeaderString(headerBuffer.AsSpan(offset), typeName);

        var columnIndex = _columnsHeadersWritten++;
        _headers[columnIndex] = headerBuffer;
        _headersLength[columnIndex] = offset;
        _data[columnIndex] = buffer;
        _dataLength[columnIndex] = dataLength;

        return columnIndex;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SetDataLength(
        ulong index,
        int length)
    {
        _dataLength[index] = length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private byte[] EnsureCapacity(
        ulong index,
        int offset,
        int required)
    {
        var buffer = _data[index];
        if (required <= buffer.Length)
        {
            return _data[index];
        }

        var newSize = Math.Max(buffer.Length * 2, required);
        var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
        buffer.AsSpan(0, offset).CopyTo(newBuffer);
        ArrayPool<byte>.Shared.Return(buffer);
        _data[index] = newBuffer;

        return newBuffer;
    }

    public ReadOnlyMemory<byte> GetWrittenBuffer()
    {
        if (_columnsHeadersWritten == 0)
        {
            return ReadOnlyMemory<byte>.Empty;
        }

        // Merge all segments into a single buffer
        var totalSizeHeaders = _headersLength.Sum(s => s);
        var totalSizeData = _dataLength.Sum(s => s);

        var merged = new byte[_blockHeaderLength + totalSizeHeaders + totalSizeData];

        _blockHeader[.._blockHeaderLength].CopyTo(merged.AsSpan(0));
        var offset = _blockHeaderLength;
        for (ulong i = 0; i < _columnsHeadersWritten; i++)
        {
            _headers[i][.._headersLength[i]].CopyTo(merged.AsSpan(offset));
            offset += _headersLength[i];
            _data[i][.._dataLength[i]].CopyTo(merged.AsSpan(offset));
            offset += _dataLength[i];
        }

        Debug.Assert(merged.Length == offset);

        return merged;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int WriteUtf8StringValue(Span<byte> destination, string value)
    {
        if (value is null)
        {
            throw new ArgumentNullException(nameof(value));
        }

        var byteCount = Encoding.UTF8.GetByteCount(value);
        var offset = WriteUVarInt(destination, (ulong)byteCount);
        offset += Encoding.UTF8.GetBytes(value.AsSpan(), destination.Slice(offset));
        return offset;
    }

    private void ReturnPooledSegments()
    {
        ArrayPool<byte>.Shared.Return(_blockHeader);

        for (ulong i = 0; i < _columnsHeadersWritten; i++)
        {
            ArrayPool<byte>.Shared.Return(_data[i]);
            ArrayPool<byte>.Shared.Return(_headers[i]);
        }
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
        return DateOnly.FromDayNumber(UnixEpochDayNumber + daysSince);
    }

    public static DateOnly From1970_01_01DaysInt32(
        ReadOnlySpan<byte> span)
    {
        var daysSince = MemoryMarshal.Read<int>(span);
        return DateOnly.FromDayNumber(UnixEpochDayNumber + daysSince);
    }

    private const int UnixEpochDayNumber = 719162;
}

public static class IpAddressExt
{
    private const int Ipv4Length = 4;

    public static IPAddress FromLittleEndianIPv4(ReadOnlySpan<byte> span)
    {
        if (span.Length < Ipv4Length)
        {
            throw new ArgumentOutOfRangeException(nameof(span), "IPv4 span must be at least 4 bytes long.");
        }

        var bigEndian = BinaryPrimitives.ReadUInt32LittleEndian(span);
        Span<byte> buffer = stackalloc byte[Ipv4Length];
        BinaryPrimitives.WriteUInt32BigEndian(buffer, bigEndian);
        return new IPAddress(buffer);
    }

    public static void WriteLittleEndianIPv4(Span<byte> destination, IPAddress value)
    {
        Span<byte> buffer = stackalloc byte[Ipv4Length];
        if (!value.TryWriteBytes(buffer, out var written) || written != Ipv4Length)
        {
            throw new InvalidOperationException("Failed to write IPv4 address bytes.");
        }

        var bigEndian = BinaryPrimitives.ReadUInt32BigEndian(buffer);
        BinaryPrimitives.WriteUInt32LittleEndian(destination, bigEndian);
    }
}
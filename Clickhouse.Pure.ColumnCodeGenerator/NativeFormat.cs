#pragma warning disable CS0414 // Field is assigned but its value is never used
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Net;

namespace Clickhouse.Pure.ColumnCodeGenerator;

public interface ISequentialColumnReader<out T>
{
    int Length { get; }
    bool HasMoreRows();
    T GetCellValueAndAdvance();
}

public interface ISequentialColumnWriter<in T>
{
    int Length { get; }
    void WriteCellValueAndAdvance(T value);
    void WriteCellValuesAndAdvance(IEnumerable<T> values);
    ReadOnlyMemory<byte> GetColumnData();
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
    private int _offset;
    private static readonly long[] Pow10 =
    [
        1L, 10L, 100L, 1_000L, 10_000L, 100_000L, 1_000_000L,
        10_000_000L, 100_000_000L, 1_000_000_000L
    ];

    private readonly ulong _columnsCount;
    private readonly ulong _rowsCount;
    private byte[] _buffer;
    private ulong _columnsWritten;

    public NativeFormatBlockWriter(
        ulong columnsCount,
        ulong rowsCount,
        int minByteSize = 969)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(minByteSize);
        _offset = 0;

        _columnsCount = columnsCount;
        _rowsCount = rowsCount;
        _columnsWritten = 0;

        WriteBlockHeader(
            columnsCount: columnsCount,
            rowsCount: rowsCount);
    }

    public void Dispose()
    {
        Free();
        GC.SuppressFinalize(this);
    }

    ~NativeFormatBlockWriter()
    {
        Free();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureCapacity(
        int additional)
    {
        if (additional < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(additional));
        }

        lock (this)
        {
            var required = _offset + additional;
            if (required <= _buffer.Length)
            {
                return;
            }

            var newSize = _buffer.Length;
            const int maxArrayLength = 2147483591;

            do
            {
                var nextSize = newSize <= maxArrayLength / 2 ? newSize * 2 : maxArrayLength;
                if (nextSize == newSize)
                {
                    // Cannot grow further.
                    if (required > newSize)
                    {
                        throw new InvalidOperationException("block too big, above 2GB");
                    }
                    break;
                }

                newSize = nextSize;
            } while (newSize < required);

            if (newSize < required)
            {
                throw new InvalidOperationException("block too big, above 2GB");
            }

            var newArr = ArrayPool<byte>.Shared.Rent(newSize);
            if (_offset != 0)
            {
                Buffer.BlockCopy(_buffer, 0, newArr, 0, _offset);
            }

            ArrayPool<byte>.Shared.Return(_buffer);

            _buffer = newArr;
        }
    }

    private void Free()
    {
        lock (this)
        {
            var buffer = _buffer;
            if (buffer == null!)
            {
                return;
            }

            _buffer = null!;
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteBlockHeader(
        ulong columnsCount,
        ulong rowsCount)
    {
        WriteUVarInt(columnsCount);
        WriteUVarInt(rowsCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteUVarInt(ulong value)
    {
        EnsureCapacity(MaxVarintLen64);

        var span = _buffer;
        var offset = _offset; // keep local for better JIT

        // Emit 7 bits at a time with the continuation bit set, until the last byte.
        while (value >= 0x80)
        {
            span[offset++] = (byte)((uint)value | 0x80); // low 7 bits + continuation
            value >>= 7;
        }
        span[offset++] = (byte)value;

        _offset = offset;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteInt64Le(
        long value)
    {
        EnsureCapacity(8);
        BinaryPrimitives.WriteInt64LittleEndian(new Span<byte>(_buffer, _offset, 8), value);
        _offset += 8;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteHeaderString(
        string str)
    {
        var byteCount = Encoding.UTF8.GetByteCount(str);
        WriteUVarInt((ulong)byteCount);
        EnsureCapacity(byteCount);

        var offset = _offset;
        _offset += Encoding.UTF8.GetBytes(
            chars: str.AsSpan(),
            bytes: new Span<byte>(_buffer, offset, byteCount));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void WriteColumnHeader(
        string columnName,
        string typeName)
    {
        if (_columnsWritten >= _columnsCount)
        {
            throw new InvalidOperationException("All declared columns have already been written.");
        }

        WriteHeaderString(columnName);
        WriteHeaderString(typeName);
        _columnsWritten++;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal int ReserveFixedSizeColumn(
        int rows,
        int valueSize)
    {
        if (rows < 0) throw new ArgumentOutOfRangeException(nameof(rows));
        if (valueSize < 0) throw new ArgumentOutOfRangeException(nameof(valueSize));

        var total = (long)rows * valueSize;
        if (total < 0 || total > int.MaxValue)
        {
            throw new InvalidOperationException("Requested column size exceeds supported limits.");
        }

        var additional = (int)total;
        EnsureCapacity(additional);
        var start = _offset;
        _offset += additional;
        return start;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal Span<byte> GetWritableSpan(
        int start,
        int length)
    {
        if ((uint)start > (uint)_buffer.Length) throw new ArgumentOutOfRangeException(nameof(start));
        if (length < 0 || start + length > _buffer.Length)
            throw new ArgumentOutOfRangeException(nameof(length));

        return new Span<byte>(_buffer, start, length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ReadOnlyMemory<byte> GetColumnSlice(
        int start,
        int length)
    {
        if ((uint)start > (uint)_buffer.Length) throw new ArgumentOutOfRangeException(nameof(start));
        if (length < 0 || start + length > _buffer.Length)
            throw new ArgumentOutOfRangeException(nameof(length));

        return new ReadOnlyMemory<byte>(_buffer, start, length);
    }

    public ReadOnlyMemory<byte> GetWrittenBuffer()
    {
        return new ReadOnlyMemory<byte>(_buffer, 0, _offset);
    }

    internal int CurrentOffset => _offset;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void WriteUtf8StringValue(string value)
    {
        if (value is null)
        {
            throw new ArgumentNullException(nameof(value));
        }

        var byteCount = Encoding.UTF8.GetByteCount(value);
        WriteUVarInt((ulong)byteCount);
        EnsureCapacity(byteCount);

        var written = Encoding.UTF8.GetBytes(
            chars: value.AsSpan(),
            bytes: new Span<byte>(_buffer, _offset, byteCount));
        _offset += written;
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

public static class IPAddressExt
{
    private const int IPv4Length = 4;

    public static IPAddress FromLittleEndianIPv4(ReadOnlySpan<byte> span)
    {
        if (span.Length < IPv4Length)
        {
            throw new ArgumentOutOfRangeException(nameof(span), "IPv4 span must be at least 4 bytes long.");
        }

        var bigEndian = BinaryPrimitives.ReadUInt32LittleEndian(span);
        Span<byte> buffer = stackalloc byte[IPv4Length];
        BinaryPrimitives.WriteUInt32BigEndian(buffer, bigEndian);
        return new IPAddress(buffer);
    }

    public static void WriteLittleEndianIPv4(Span<byte> destination, IPAddress value)
    {
        Span<byte> buffer = stackalloc byte[IPv4Length];
        if (!value.TryWriteBytes(buffer, out var written) || written != IPv4Length)
        {
            throw new InvalidOperationException("Failed to write IPv4 address bytes.");
        }

        var bigEndian = BinaryPrimitives.ReadUInt32BigEndian(buffer);
        BinaryPrimitives.WriteUInt32LittleEndian(destination, bigEndian);
    }
}
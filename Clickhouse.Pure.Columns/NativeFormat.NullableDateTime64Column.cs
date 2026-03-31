#nullable enable
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;

namespace Clickhouse.Pure.Columns;

public partial class NativeFormatBlockReader
{
    public NullableDateTime64ColumnReader ReadNullableDateTime64Column(int scale, string timeZone)
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;

        if (type.Length < 21 || !MatchesType(type[..20], "Nullable(DateTime64("u8))
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected Nullable(DateTime64(...)) for column '{Encoding.UTF8.GetString(name)}', but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return NullableDateTime64ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount, scale, timeZone);
    }

    public ref struct NullableDateTime64ColumnReader : ISequentialColumnReader<DateTimeOffset?>
    {
        private const int ValueSize = 8;
        private readonly ReadOnlySpan<byte> _nullsMask;
        private readonly ReadOnlySpan<byte> _data;
        private int _valuesOffset;
        private readonly int _rows;
        private int _index;
        private readonly int _scale;
        private readonly TimeZoneInfo _tz;

        private NullableDateTime64ColumnReader(
            ReadOnlySpan<byte> nullsMask, ReadOnlySpan<byte> data, int valuesOffset,
            int rows, int scale, string timeZone)
        {
            _nullsMask = nullsMask;
            _data = data;
            _valuesOffset = valuesOffset;
            _rows = rows;
            _scale = scale;
            _tz = string.IsNullOrEmpty(timeZone) ? TimeZoneInfo.Utc : TimeZoneInfo.FindSystemTimeZoneById(timeZone);
            _index = 0;
        }

        public int Length => _rows;
        public bool HasMoreRows() => _index < _rows;

        public static NullableDateTime64ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data, scoped ref int offset, int rows, int scale, string timeZone)
        {
            if (offset + rows > data.Length) throw new IndexOutOfRangeException("nullable nulls out of range");
            var mask = data.Slice(offset, rows);
            var valuesStart = offset + rows;
            var total = (long)rows * ValueSize;
            if (valuesStart + total > data.Length) throw new IndexOutOfRangeException("nullable DateTime64 values out of range");
            offset = valuesStart + (int)total;
            return new NullableDateTime64ColumnReader(mask, data, valuesStart, rows, scale, timeZone);
        }

        public DateTimeOffset? ReadNext()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            var isNull = _nullsMask[_index] != 0;
            _index++;

            var raw = BinaryPrimitives.ReadInt64LittleEndian(_data.Slice(_valuesOffset, ValueSize));
            _valuesOffset += ValueSize;

            if (isNull) return null;

            if ((uint)_scale > 9)
                throw new ArgumentOutOfRangeException(nameof(_scale), "DateTime64 precision must be in [0..9].");

            var pow = Pow10[_scale];
            var seconds = Math.DivRem(raw, pow, out var rem);
            if (rem < 0) { seconds--; rem += pow; }

            var fracTicks = (rem * TimeSpan.TicksPerSecond) / pow;
            var totalTicks = checked(seconds * TimeSpan.TicksPerSecond + fracTicks);
            var dtoUtc = new DateTimeOffset(DateTime.UnixEpoch.AddTicks(totalTicks), TimeSpan.Zero);

            return _tz.Equals(TimeZoneInfo.Utc) ? dtoUtc : TimeZoneInfo.ConvertTime(dtoUtc, _tz);
        }
    }
}

public partial class NativeFormatBlockWriter
{
    public NullableDateTime64ColumnWriter CreateNullableDateTime64ColumnWriter(string columnName, int scale, string timeZone)
    {
        if ((uint)scale > 9)
        {
            throw new ArgumentOutOfRangeException(nameof(scale), "DateTime64 precision must be in [0..9].");
        }

        var innerType = string.IsNullOrEmpty(timeZone)
            ? $"DateTime64({scale})"
            : $"DateTime64({scale}, '{timeZone}')";
        var typeName = $"Nullable({innerType})";

        return NullableDateTime64ColumnWriter.Create(this, columnName, typeName, checked((int)_rowsCount), scale);
    }

    public ref struct NullableDateTime64ColumnWriter : ISequentialColumnWriter<DateTimeOffset?, NullableDateTime64ColumnWriter>
    {
        private const int ValueSize = 8;
        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly int _maskLength;
        private readonly byte[] _buffer;
        private readonly long _pow;
        private int _index;

        private NullableDateTime64ColumnWriter(
            ulong blockIndex, NativeFormatBlockWriter writer, int rows, byte[] buffer, int scale)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _maskLength = rows;
            _buffer = buffer;
            _pow = Pow10[scale];
            _index = 0;
            _buffer.AsSpan(0, _maskLength + rows * ValueSize).Clear();
            _writer.SetDataLength(_blockIndex, _maskLength);
        }

        internal static NullableDateTime64ColumnWriter Create(
            NativeFormatBlockWriter writer, string columnName, string typeName, int rows, int scale)
        {
            var totalSize = rows + rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(buffer, columnName, typeName, 0);
            return new NullableDateTime64ColumnWriter(blockIndex, writer, rows, buffer, scale);
        }

        public NullableDateTime64ColumnWriter WriteNext(DateTimeOffset? value)
        {
            if (_index >= _rows) throw new InvalidOperationException("No more rows to write.");

            if (value is null)
            {
                _buffer[_index] = 1;
            }
            else
            {
                _buffer[_index] = 0;
                var utc = value.Value.ToUniversalTime();
                var ticksSinceEpoch = utc.UtcTicks - DateTime.UnixEpoch.Ticks;
                var seconds = Math.DivRem(ticksSinceEpoch, TimeSpan.TicksPerSecond, out var remainderTicks);
                if (remainderTicks < 0) { seconds--; remainderTicks += TimeSpan.TicksPerSecond; }
                var fractional = (_pow * remainderTicks) / TimeSpan.TicksPerSecond;
                var raw = checked(seconds * _pow + fractional);

                var dest = _buffer.AsSpan(_maskLength + _index * ValueSize, ValueSize);
                BinaryPrimitives.WriteInt64LittleEndian(dest, raw);
            }

            _index++;
            _writer.SetDataLength(_blockIndex, _maskLength + _index * ValueSize);
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<DateTimeOffset?> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var v in values) WriteNext(v);
            return _writer;
        }
    }
}

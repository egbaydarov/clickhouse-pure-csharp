using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;

namespace Clickhouse.Pure.Columns;

public partial class NativeFormatBlockReader
{
    public DateTime64ColumnReader ReadDateTime64Column(int scale, string timeZone)
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        if (!MatchesType(type[..11], "DateTime64("u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected DateTime64 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return DateTime64ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount, scale, timeZone);
    }

    public ref struct DateTime64ColumnReader : ISequentialColumnReader<DateTimeOffset>
    {
        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private readonly int _rows;
        private int _index;
        private readonly int _scale; // decimal places of second
        private readonly TimeZoneInfo _tz;

        private DateTime64ColumnReader(ReadOnlySpan<byte> data, int startOffset, int rows, int scale, string timeZone)
        {
            _data = data;
            _offset = startOffset;
            _rows = rows;
            _scale = scale;
            _tz = string.IsNullOrEmpty(timeZone) ? TimeZoneInfo.Utc : TimeZoneInfo.FindSystemTimeZoneById(timeZone);
            _index = 0;
        }

        public static DateTime64ColumnReader CreateAndConsume(ReadOnlySpan<byte> data, scoped ref int offset, int rows,
            int scale, string timeZone)
        {
            var start = offset;
            var total = (long)rows * 8;
            if (total < 0 || start + total > data.Length)
                throw new IndexOutOfRangeException("DateTime64 column out of range");
            offset = start + (int)total;
            return new DateTime64ColumnReader(data, start, rows, scale, timeZone);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public DateTimeOffset ReadNext()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            _index++;

            if (_offset + 8 > _data.Length) throw new IndexOutOfRangeException("DateTime64 out of range");

            // DateTime64 stores a SIGNED Int64 count of 10^-scale seconds since Unix epoch.
            var raw = BinaryPrimitives.ReadInt64LittleEndian(_data.Slice(_offset, 8));
            _offset += 8;

            if ((uint)_scale > 9)
                throw new ArgumentOutOfRangeException(nameof(_scale), "DateTime64 precision must be in [0..9].");

            // integer powers of 10 to avoid Math.Pow(double) precision issues
            var pow = Pow10[_scale];

            var seconds = Math.DivRem(raw, pow, out var rem);

            // Normalize for negatives so 0 <= rem < pow and seconds = floor(raw/pow)
            if (rem < 0)
            {
                seconds--;
                rem += pow;
            }

            // TRUNCATE sub-second part to .NET ticks (100 ns). No rounding up into next second.
            var fracTicks = (rem * TimeSpan.TicksPerSecond) / pow; // safe: rem<=1e9-1 => product <= 1e16-1
            var totalTicks = checked(seconds * TimeSpan.TicksPerSecond + fracTicks);
            var dtoUtc = new DateTimeOffset(DateTime.UnixEpoch.AddTicks(totalTicks), TimeSpan.Zero);

            return _tz.Equals(TimeZoneInfo.Utc)
                ? dtoUtc
                : TimeZoneInfo.ConvertTime(dtoUtc, _tz);
        }
    }
}

public partial class NativeFormatBlockWriter
{
    public DateTime64ColumnWriter CreateDateTime64ColumnWriter(string columnName, int scale, string timeZone)
    {
        if ((uint)scale > 9)
        {
            throw new ArgumentOutOfRangeException(nameof(scale), "DateTime64 precision must be in [0..9].");
        }

        var typeName = string.IsNullOrEmpty(timeZone)
            ? $"DateTime64({scale})"
            : $"DateTime64({scale}, '{timeZone}')";
        WriteColumnHeader(columnName, typeName);

        return DateTime64ColumnWriter.Create(this, checked((int)_rowsCount), scale);
    }

    public ref struct DateTime64ColumnWriter : ISequentialColumnWriter<DateTimeOffset, DateTime64ColumnWriter>
    {
        private const int ValueSize = 8;

        private NativeFormatBlockWriter _writer;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private readonly long _pow;
        private int _index;
        private bool _segmentAdded;

        private DateTime64ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer,
            int scale)
        {
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _pow = Pow10[scale];
            _index = 0;
            _segmentAdded = false;
        }

        internal static DateTime64ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows,
            int scale)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            return new DateTime64ColumnWriter(writer, rows, buffer, scale);
        }

        public int Length => _rows;

        public DateTime64ColumnWriter WriteNext(DateTimeOffset value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var utc = value.ToUniversalTime();
            var ticksSinceEpoch = utc.UtcTicks - DateTime.UnixEpoch.Ticks;

            var seconds = Math.DivRem(ticksSinceEpoch, TimeSpan.TicksPerSecond, out var remainderTicks);
            if (remainderTicks < 0)
            {
                seconds--;
                remainderTicks += TimeSpan.TicksPerSecond;
            }

            var fractional = (_pow * remainderTicks) / TimeSpan.TicksPerSecond;
            var raw = checked(seconds * _pow + fractional);

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            BinaryPrimitives.WriteInt64LittleEndian(dest, raw);
            _index++;

            if (_index == _rows)
            {
                EnsureSegmentAdded();
            }

            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<DateTimeOffset> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            if (_index == _rows)
            {
                EnsureSegmentAdded();
            }

            return _writer;
        }

        public ReadOnlyMemory<byte> GetColumnData()
        {
            if (_index != _rows)
            {
                throw new InvalidOperationException("Attempted to get column data before all rows were written.");
            }

            EnsureSegmentAdded();
            return new ReadOnlyMemory<byte>(_buffer, 0, _rows * ValueSize);
        }

        private void EnsureSegmentAdded()
        {
            if (_segmentAdded)
            {
                return;
            }

            var segment = new ReadOnlyMemory<byte>(_buffer, 0, _rows * ValueSize);
            _writer.AddSegment(segment, _buffer);
            _segmentAdded = true;
        }
    }
}

using System.Buffers.Binary;
using System.Text;

namespace Clickhouse.Pure.ColumnCodeGenerator;

public partial class NativeFormatBlockReader
{
    public DateTime64ColumnReader AdvanceDateTime64Column(int scale, string timeZone)
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

        return DateTime64ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount, scale, timeZone);
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

        public static DateTime64ColumnReader CreateAndAdvance(ReadOnlySpan<byte> data, scoped ref int offset, int rows,
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

        public DateTimeOffset GetCellValueAndAdvance()
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
            var dtUtc = DateTime.UnixEpoch.AddTicks(totalTicks); // Kind=Utc

            return _tz.Equals(TimeZoneInfo.Utc)
                ? dtUtc
                : TimeZoneInfo.ConvertTimeFromUtc(dtUtc, _tz);
        }
    }
}

public partial class NativeFormatBlockWriter
{
    public DateTime64ColumnWriter AdvanceDateTime64ColumnWriter(string columnName, int scale, string timeZone)
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

    public ref struct DateTime64ColumnWriter : ISequentialColumnWriter<DateTimeOffset>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _rows;
        private readonly int _startOffset;
        private readonly int _scale;
        private readonly long _pow;
        private int _index;

        private DateTime64ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset,
            int scale)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _scale = scale;
            _pow = Pow10[scale];
            _index = 0;
        }

        internal static DateTime64ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows,
            int scale)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 8);
            return new DateTime64ColumnWriter(writer, rows, startOffset, scale);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(DateTimeOffset value)
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

            var dest = _writer.GetWritableSpan(_startOffset + _index * 8, 8);
            BinaryPrimitives.WriteInt64LittleEndian(dest, raw);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<DateTimeOffset> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteCellValueAndAdvance(value);
            }
        }

        public ReadOnlyMemory<byte> GetColumnData()
        {
            if (_index != _rows)
            {
                throw new InvalidOperationException("Attempted to get column data before all rows were written.");
            }

            return _writer.GetColumnSlice(_startOffset, _rows * 8);
        }
    }
}

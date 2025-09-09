using System;
using System.Buffers.Binary;
using System.Text;
using Clickhouse.Pure.ColumnCodeGenerator;

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

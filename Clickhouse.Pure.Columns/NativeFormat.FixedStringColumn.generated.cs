#nullable enable
using System;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Collections.Generic;

namespace Clickhouse.Pure.Columns;

public partial class NativeFormatBlockReader
{
    public FixedStringColumnReader AdvanceFixedStringColumn()
    {
        // Type header is like FixedString(N)
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type.Slice(0, 12), "FixedString()"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected FixedString for column '{Encoding.UTF8.GetString(name)}', but got '{Encoding.UTF8.GetString(type)}'.");
        }

        if (Utf8Parser.TryParse(
                source: type.Slice(12,
                length: type.Length - 12),
                value: out int value,
                bytesConsumed: out _))
        {
            return FixedStringColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount, value);
        }

        throw new InvalidOperationException($"Column type mismatch. Expected FixedString(number) for column '{Encoding.UTF8.GetString(name)}', but got '{Encoding.UTF8.GetString(type)}'.");
    }

    public ref struct FixedStringColumnReader : ISequentialColumnReader<string>
    {
        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private readonly int _rows;
        private int _index;
        private readonly int _fixedLen;

        private FixedStringColumnReader(ReadOnlySpan<byte> data, int startOffset, int rows, int fixedLen)
        {
            _data = data;
            _offset = startOffset;
            _rows = rows;
            _fixedLen = fixedLen;
            _index = 0;
        }

        public static FixedStringColumnReader CreateAndAdvance(ReadOnlySpan<byte> data, scoped ref int offset, int rows,
            int fixedLen)
        {
            var start = offset;
            var total = (long)rows * fixedLen;
            if (total < 0 || start + total > data.Length)
                throw new IndexOutOfRangeException("FixedString column out of range");
            offset = start + (int)total;
            return new FixedStringColumnReader(data, start, rows, fixedLen);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public string GetCellValueAndAdvance()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            _index++;
            if (_offset + _fixedLen > _data.Length)
            {
                throw new IndexOutOfRangeException("fixed string out of range");
            }

            var slice = _data.Slice(_offset, _fixedLen);
            _offset += _fixedLen;

            // trim trailing zeros
            var end = slice.Length;
            while (end > 0 && slice[end - 1] == 0) end--;

            return Encoding.UTF8.GetString(slice.Slice(0, end));
        }
    }
}

public partial class NativeFormatBlockWriter
{
    public FixedStringColumnWriter AdvanceFixedStringColumnWriter(string columnName, int size)
    {
        if (size <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(size), "FixedString size must be a positive integer.");
        }

        var typeName = $"FixedString({size})";
        WriteColumnHeader(columnName, typeName);
        return FixedStringColumnWriter.Create(this, checked((int)_rowsCount), size);
    }

    public ref struct FixedStringColumnWriter : ISequentialColumnWriter<string>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _rows;
        private readonly int _startOffset;
        private readonly int _size;
        private int _index;

        private FixedStringColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset,
            int size)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _size = size;
            _index = 0;
        }

        internal static FixedStringColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows,
            int size)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, size);
            return new FixedStringColumnWriter(writer, rows, startOffset, size);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(string value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var byteCount = Encoding.UTF8.GetByteCount(value);
            if (byteCount > _size)
            {
                throw new ArgumentOutOfRangeException(nameof(value), $"FixedString value exceeds size {_size}.");
            }

            var dest = _writer.GetWritableSpan(
                _startOffset + _index * _size,
                _size);
            dest.Clear();
            if (byteCount > 0)
            {
                _ = Encoding.UTF8.GetBytes(value.AsSpan(), dest.Slice(0, byteCount));
            }

            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<string> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * _size);
        }
    }
}
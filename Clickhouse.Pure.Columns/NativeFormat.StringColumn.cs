using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace Clickhouse.Pure.Columns;

public partial class NativeFormatBlockReader
{
    public StringColumnReader ReadStringColumn()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();

        _columnsRead++;
        if (!MatchesType(type, "String"u8))
            throw new InvalidOperationException($"Column type mismatch. Expected String for column '{Encoding.UTF8.GetString(name)}',  but got '{Encoding.UTF8.GetString(type)}'.");
        return StringColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct StringColumnReader : ISequentialColumnReader<string>
    {
        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private readonly int _rows;
        private int _index;

        private StringColumnReader(ReadOnlySpan<byte> data, int startOffset, int rows)
        {
            _data = data;
            _offset = startOffset;
            _rows = rows;
            _index = 0;
        }

        public static StringColumnReader CreateAndConsume(ReadOnlySpan<byte> data, scoped ref int offset, int rows)
        {
            var start = offset;
            var tmp = offset;
            for (var i = 0; i < rows; i++)
            {
                var len = (int)ReadUVarInt(ref tmp, data);
                tmp += len;
                if (tmp > data.Length) throw new IndexOutOfRangeException("String column out of range while scanning");
            }

            offset = tmp;
            return new StringColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public string ReadNext()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            _index++;
            var len = (int)ReadUVarInt(ref _offset, _data);
            if (_offset + len > _data.Length) throw new IndexOutOfRangeException("string out of range");
            var s = Encoding.UTF8.GetString(_data.Slice(_offset, len));
            _offset += len;
            return s;
        }
    }
}

public partial class NativeFormatBlockWriter
{
    public StringColumnWriter CreateStringColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "String");
        return StringColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct StringColumnWriter : ISequentialColumnWriter<string, StringColumnWriter>
    {
        private const int InitialCapacity = 1024;

        private NativeFormatBlockWriter _writer;
        private readonly int _rows;
        private byte[] _buffer;
        private int _offset;
        private int _index;
        private bool _segmentAdded;

        private StringColumnWriter(NativeFormatBlockWriter writer, int rows, byte[] buffer)
        {
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _offset = 0;
            _index = 0;
            _segmentAdded = false;
        }

        internal static StringColumnWriter Create(NativeFormatBlockWriter writer, int rows)
        {
            var initial = Math.Max(InitialCapacity, rows * 4);
            var buffer = ArrayPool<byte>.Shared.Rent(initial);
            return new StringColumnWriter(writer, rows, buffer);
        }

        public int Length => _rows;

        public StringColumnWriter WriteNext(string value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var byteCount = Encoding.UTF8.GetByteCount(value);
            EnsureCapacity(_offset + NativeFormatBlockWriter.MaxVarintLen64 + byteCount);

            _offset += NativeFormatBlockWriter.WriteUtf8StringValue(_buffer.AsSpan(_offset), value);
            _index++;

            if (_index == _rows)
            {
                EnsureSegmentAdded();
            }

            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<string> values)
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
            return new ReadOnlyMemory<byte>(_buffer, 0, _offset);
        }

        private void EnsureCapacity(int required)
        {
            if (required <= _buffer.Length)
            {
                return;
            }

            var newSize = Math.Max(_buffer.Length * 2, required);
            var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
            _buffer.AsSpan(0, _offset).CopyTo(newBuffer);
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = newBuffer;
        }

        private void EnsureSegmentAdded()
        {
            if (_segmentAdded)
            {
                return;
            }

            var segment = new ReadOnlyMemory<byte>(_buffer, 0, _offset);
            _writer.AddSegment(segment, _buffer);
            _segmentAdded = true;
        }
    }
}

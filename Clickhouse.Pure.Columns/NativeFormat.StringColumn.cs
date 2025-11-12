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
        return StringColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct StringColumnWriter : ISequentialColumnWriter<string, StringColumnWriter>
    {
        private const int InitialCapacity = 1024;

        private readonly ulong _blockIndex;
        private NativeFormatBlockWriter _writer;
        private readonly int _rows;
        private byte[] _buffer;
        private int _offset;
        private int _index;

        private StringColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _offset = 0;
            _index = 0;
        }

        internal static StringColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var initial = Math.Max(InitialCapacity, rows * 4);
            var buffer = ArrayPool<byte>.Shared.Rent(initial);

            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "String",
                dataLength: 0);

            return new StringColumnWriter(
                blockIndex,
                writer,
                rows,
                buffer);
        }

        public StringColumnWriter WriteNext(string value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var byteCount = Encoding.UTF8.GetByteCount(value);
            _buffer = _writer.EnsureCapacity(
                index: _blockIndex,
                offset: _offset,
                required: _offset + MaxVarintLen64 + byteCount);

            _offset += WriteUtf8StringValue(_buffer.AsSpan(_offset), value);
            _index++;

            _writer.SetDataLength(_blockIndex, _offset);

            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<string> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }
    }
}

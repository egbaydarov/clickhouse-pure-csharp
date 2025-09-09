using System;
using System.Text;
using Clickhouse.Pure.ColumnCodeGenerator;

namespace Clickhouse.Pure.ColumnCodeGenerator;

public partial class NativeFormatBlockReader
{
    public StringColumnReader AdvanceStringColumn()
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
        return StringColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static StringColumnReader CreateAndAdvance(ReadOnlySpan<byte> data, scoped ref int offset, int rows)
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

        public string GetCellValueAndAdvance()
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

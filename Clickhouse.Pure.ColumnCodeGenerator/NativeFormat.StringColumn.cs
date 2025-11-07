using System.Text;

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

public partial class NativeFormatBlockWriter
{
    public StringColumnWriter AdvanceStringColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "String");
        return StringColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct StringColumnWriter : ISequentialColumnWriter<string>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _rows;
        private readonly int _dataStart;
        private int _index;
        private int _dataEnd;

        private StringColumnWriter(NativeFormatBlockWriter writer, int rows, int dataStart)
        {
            _writer = writer;
            _rows = rows;
            _dataStart = dataStart;
            _index = 0;
            _dataEnd = -1;
        }

        internal static StringColumnWriter Create(NativeFormatBlockWriter writer, int rows)
        {
            return new StringColumnWriter(writer, rows, writer.CurrentOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(string value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            _writer.WriteUtf8StringValue(value);
            _index++;

            if (_index == _rows)
            {
                _dataEnd = _writer.CurrentOffset;
            }
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

            if (_dataEnd < 0)
            {
                _dataEnd = _writer.CurrentOffset;
            }

            return _writer.GetColumnSlice(_dataStart, _dataEnd - _dataStart);
        }
    }
}

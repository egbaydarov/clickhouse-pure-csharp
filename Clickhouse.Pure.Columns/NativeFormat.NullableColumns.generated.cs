#nullable enable
using System;
using System.Buffers.Binary;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Collections.Generic;

namespace Clickhouse.Pure.Columns;

public partial class NativeFormatBlockReader
{
    public NullableStringColumnReader AdvanceNullableStringColumn()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();

        if (!MatchesType(type, "Nullable(String)"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected Nullable(String) for column '{Encoding.UTF8.GetString(name)}',  but got '{Encoding.UTF8.GetString(type)}'.");
        }
        return NullableStringColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct NullableStringColumnReader : ISequentialColumnReader<string?>
    {
        private readonly ReadOnlySpan<byte> _data;
        private int _offsetValues;
        private readonly int _rows;
        private int _index;
        private readonly ReadOnlySpan<byte> _nullsMask;

        private NullableStringColumnReader(ReadOnlySpan<byte> data, int startNullsOffset, int rows,
            ReadOnlySpan<byte> nullsMask)
        {
            _data = data;
            _offsetValues = startNullsOffset;
            _rows = rows;
            _nullsMask = nullsMask;
            _index = 0;
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public static NullableStringColumnReader CreateAndAdvance(ReadOnlySpan<byte> data, scoped ref int offset,
            int rows)
        {
            // Nulls mask first: one byte per row (1 = NULL, 0 = not null)
            if (offset + rows > data.Length) throw new IndexOutOfRangeException("nullable nulls out of range");
            var mask = data.Slice(offset, rows);
            var valuesStart = offset + rows;
            var local = valuesStart;

            // IMPORTANT: Values are encoded for every row regardless of null mask (nulls typically encoded as empty values).
            // Always read length and advance for each row, matching server encoding.
            for (var i = 0; i < rows; i++)
            {
                var len = (int)ReadUVarInt(ref local, data);
                local += len;
                if (local > data.Length)
                    throw new IndexOutOfRangeException("nullable string values out of range while scanning");
            }

            offset = local;
            return new NullableStringColumnReader(data, valuesStart, rows, mask);
        }

        public string? GetCellValueAndAdvance()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            var isNull = _nullsMask[_index] != 0;
            _index++;
            var len = (int)ReadUVarInt(ref _offsetValues, _data);
            if (_offsetValues + len > _data.Length)
                throw new IndexOutOfRangeException("nullable string value out of range");
            if (isNull)
            {
                _offsetValues += len;
                return null;
            }

            var valueSpan = _data.Slice(_offsetValues, len); 
            _offsetValues += len;
            return Encoding.UTF8.GetString(valueSpan);
        }
    }
}

public partial class NativeFormatBlockWriter
{
    public NullableStringColumnWriter AdvanceNullableStringColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "Nullable(String)");
        return NullableStringColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct NullableStringColumnWriter : ISequentialColumnWriter<string?>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _rows;
        private readonly int _maskStart;
        private readonly int _dataStart;
        private int _index;
        private int _dataEnd;

        private NullableStringColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int maskStart)
        {
            _writer = writer;
            _rows = rows;
            _maskStart = maskStart;
            _dataStart = maskStart;
            _index = 0;
            _dataEnd = -1;
        }

        internal static NullableStringColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var maskStart = writer.ReserveFixedSizeColumn(rows, 1);
            return new NullableStringColumnWriter(writer, rows, maskStart);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(string? value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var flag = _writer.GetWritableSpan(_maskStart + _index, 1);
            if (value is null)
            {
                flag[0] = 1;
                _writer.WriteUVarInt(0);
            }
            else
            {
                flag[0] = 0;
                _writer.WriteUtf8StringValue(value);
            }

            _index++;
            if (_index == _rows)
            {
                _dataEnd = _writer.CurrentOffset;
            }
        }

        public void WriteCellValuesAndAdvance(IEnumerable<string?> values)
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
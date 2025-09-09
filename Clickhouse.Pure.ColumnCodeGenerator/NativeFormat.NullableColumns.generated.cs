#nullable enable
using System.Buffers.Binary;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Clickhouse.Pure.ColumnCodeGenerator;

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

            _offsetValues += len;
            return Encoding.UTF8.GetString(_data.Slice(_offsetValues, len));
        }
    }
}
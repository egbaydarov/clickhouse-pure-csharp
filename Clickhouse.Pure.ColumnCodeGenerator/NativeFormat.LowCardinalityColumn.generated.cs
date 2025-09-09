#nullable enable
using System.Buffers.Binary;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Clickhouse.Pure.ColumnCodeGenerator;

public partial class NativeFormatBlockReader
{
    public LowCardinalityStringColumnReader AdvanceLowCardinalityStringColumn()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;

        if (!MatchesType(type, "LowCardinality(String)"u8))
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected LowCardinality(String) for column '{Encoding.UTF8.GetString(name)}', but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return LowCardinalityStringColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct LowCardinalityStringColumnReader : ISequentialColumnReader<string>
    {
        private readonly string[] _dictionary;
        private readonly int _rows;
        private readonly int _keyWidthBytes; // 1, 2, 4, or 8
        private readonly ReadOnlySpan<byte> _keysData;
        private int _index;

        private LowCardinalityStringColumnReader(string[] dictionary, ReadOnlySpan<byte> keysData, int rows,
            int keyWidthBytes)
        {
            _dictionary = dictionary;
            _rows = rows;
            _keysData = keysData;
            _keyWidthBytes = keyWidthBytes;
            _index = 0;
        }

        public static LowCardinalityStringColumnReader CreateAndAdvance(ReadOnlySpan<byte> data, scoped ref int offset,
            int rows)
        {
            // Format per ClickHouse Native (see ch-go proto ColLowCardinality):
            // meta:int64 (LE) with flags and key type
            // indexRows:int64 (LE)
            // index values: String[indexRows]
            // keyRows:int64 (LE)
            // keys: UInt{8|16|32|64}[keyRows]

            var local = offset;

            var first64 = ReadInt64Le(ref local, data);
            var meta = first64 == 1 ? ReadInt64Le(ref local, data) : first64;

            // Bits
            const long cardinalityKeyMask = 0xFF; // last byte
            const long cardinalityNeedGlobalDictionaryBit = 1L << 8;
            const long cardinalityHasAdditionalKeysBit = 1L << 9;

            if ((meta & cardinalityNeedGlobalDictionaryBit) != 0)
            {
                throw new NotSupportedException("LowCardinality global dictionary is not supported");
            }

            if ((meta & cardinalityHasAdditionalKeysBit) == 0)
            {
                // Continue best-effort; some versions skip flag.
            }

            var keyType = (int)(meta & cardinalityKeyMask); // 0=UInt8,1=UInt16,2=UInt32,3=UInt64
            var width = keyType switch
            {
                0 => 1,
                1 => 2,
                2 => 4,
                3 => 8,
                _ => throw new InvalidOperationException($"Invalid LowCardinality key type {keyType}")
            };

            var indexRows64 = ReadInt64Le(ref local, data);
            if (indexRows64 < 0 || indexRows64 > int.MaxValue)
                throw new OverflowException("dictionary size out of range");
            var dictSize = (int)indexRows64;

            var dict = new string[dictSize];
            for (var i = 0; i < dictSize; i++)
            {
                var len = (int)ReadUVarInt(ref local, data);
                if (local + len > data.Length) throw new IndexOutOfRangeException("lowcard dict value out of range");
                dict[i] = Encoding.UTF8.GetString(data.Slice(local, len));
                local += len;
            }

            var keyRows64 = ReadInt64Le(ref local, data);
            if (keyRows64 < 0 || keyRows64 > int.MaxValue) throw new OverflowException("keys size out of range");
            var keyRows = (int)keyRows64;

            if (keyRows != rows)
            {
                // In practice should be equal; if not, keep using reported key rows
                throw new IndexOutOfRangeException("lowcard dict value out of range");
            }

            var keysBytesTotal = checked(rows * width);
            if (local + keysBytesTotal > data.Length) throw new IndexOutOfRangeException("lowcard keys out of range");
            var keysData = data.Slice(local, keysBytesTotal);
            local += keysBytesTotal;

            offset = local;
            return new LowCardinalityStringColumnReader(dict, keysData, rows, width);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public string GetCellValueAndAdvance()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            int key;
            var pos = _index * _keyWidthBytes;
            switch (_keyWidthBytes)
            {
                case 1: key = _keysData[pos]; break;
                case 2: key = BinaryPrimitives.ReadUInt16LittleEndian(_keysData.Slice(pos, 2)); break;
                case 4: key = (int)BinaryPrimitives.ReadUInt32LittleEndian(_keysData.Slice(pos, 4)); break;
                case 8:
                    {
                        var v = BinaryPrimitives.ReadUInt64LittleEndian(_keysData.Slice(pos, 8));
                        if (v > int.MaxValue) throw new OverflowException("lowcard key does not fit into int");
                        key = (int)v;
                        break;
                    }
                default: throw new InvalidOperationException("invalid key width");
            }

            _index++;
            if ((uint)key >= (uint)_dictionary.Length)
                throw new IndexOutOfRangeException("lowcard key out of dict bounds");
            return _dictionary[key];
        }
    }
}
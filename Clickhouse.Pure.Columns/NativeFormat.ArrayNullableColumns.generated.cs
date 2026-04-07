#nullable enable
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;

namespace Clickhouse.Pure.Columns;

public partial class NativeFormatBlockReader
{
    public ArrayNullableStringColumnReader ReadArrayNullableStringColumn()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;

        if (!MatchesType(type, "Array(Nullable(String))"u8))
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected Array(Nullable(String)) for column '{Encoding.UTF8.GetString(name)}', but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return ArrayNullableStringColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct ArrayNullableStringColumnReader : ISequentialColumnReader<string?[]>
    {
        private readonly long[] _offsets;
        private readonly string?[] _values;
        private readonly int _rows;
        private int _index;

        private ArrayNullableStringColumnReader(long[] offsets, string?[] values, int rows)
        {
            _offsets = offsets;
            _values = values;
            _rows = rows;
            _index = 0;
        }

        public int Length => _rows;
        public bool HasMoreRows() => _index < _rows;

        public static ArrayNullableStringColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data, scoped ref int offset, int rows)
        {
            var local = offset;

            if (local + rows * 8 > data.Length) throw new IndexOutOfRangeException("array offsets out of range");
            var offsets = new long[rows];
            for (var i = 0; i < rows; i++)
            {
                offsets[i] = (long)BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(local, 8));
                local += 8;
            }

            var totalElements = rows > 0 ? (int)offsets[rows - 1] : 0;

            if (local + totalElements > data.Length) throw new IndexOutOfRangeException("array inner mask out of range");
            var mask = data.Slice(local, totalElements);
            local += totalElements;

            var values = new string?[totalElements];
            for (var i = 0; i < totalElements; i++)
            {
                var len = (int)ReadUVarInt(ref local, data);
                if (local + len > data.Length) throw new IndexOutOfRangeException("array inner string out of range");
                if (mask[i] != 0)
                {
                    values[i] = null;
                }
                else
                {
                    values[i] = Encoding.UTF8.GetString(data.Slice(local, len));
                }
                local += len;
            }

            offset = local;
            return new ArrayNullableStringColumnReader(offsets, values, rows);
        }

        public string?[] ReadNext()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            var start = _index == 0 ? 0 : (int)_offsets[_index - 1];
            var end = (int)_offsets[_index];
            _index++;

            var result = new string?[end - start];
            Array.Copy(_values, start, result, 0, result.Length);
            return result;
        }
    }
    public ArrayNullableUInt32ColumnReader ReadArrayNullableUInt32Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;

        if (!MatchesType(type, "Array(Nullable(UInt32))"u8))
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected Array(Nullable(UInt32)) for column '{Encoding.UTF8.GetString(name)}', but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return ArrayNullableUInt32ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct ArrayNullableUInt32ColumnReader : ISequentialColumnReader<uint?[]>
    {
        private const int ValueSize = 4;
        private readonly long[] _offsets;
        private readonly uint?[] _values;
        private readonly int _rows;
        private int _index;

        private ArrayNullableUInt32ColumnReader(long[] offsets, uint?[] values, int rows)
        {
            _offsets = offsets;
            _values = values;
            _rows = rows;
            _index = 0;
        }

        public int Length => _rows;
        public bool HasMoreRows() => _index < _rows;

        public static ArrayNullableUInt32ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data, scoped ref int offset, int rows)
        {
            var local = offset;

            if (local + rows * 8 > data.Length) throw new IndexOutOfRangeException("array offsets out of range");
            var offsets = new long[rows];
            for (var i = 0; i < rows; i++)
            {
                offsets[i] = (long)BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(local, 8));
                local += 8;
            }

            var totalElements = rows > 0 ? (int)offsets[rows - 1] : 0;

            if (local + totalElements > data.Length) throw new IndexOutOfRangeException("array inner mask out of range");
            var mask = data.Slice(local, totalElements);
            local += totalElements;

            if (local + totalElements * ValueSize > data.Length) throw new IndexOutOfRangeException("array inner values out of range");
            var values = new uint?[totalElements];
            for (var i = 0; i < totalElements; i++)
            {
                if (mask[i] != 0)
                {
                    values[i] = null;
                }
                else
                {
                    values[i] = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(local, ValueSize));
                }
                local += ValueSize;
            }

            offset = local;
            return new ArrayNullableUInt32ColumnReader(offsets, values, rows);
        }

        public uint?[] ReadNext()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            var start = _index == 0 ? 0 : (int)_offsets[_index - 1];
            var end = (int)_offsets[_index];
            _index++;

            var result = new uint?[end - start];
            Array.Copy(_values, start, result, 0, result.Length);
            return result;
        }
    }
}

public partial class NativeFormatBlockWriter
{
    public ArrayNullableStringColumnWriter CreateArrayNullableStringColumnWriter(string columnName)
    {
        return ArrayNullableStringColumnWriter.Create(this, columnName, checked((int)_rowsCount));
    }

    public ref struct ArrayNullableStringColumnWriter : ISequentialColumnWriter<string?[], ArrayNullableStringColumnWriter>
    {
        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly List<string?[]> _collected;
        private byte[] _buffer;
        private int _index;
        private bool _encoded;

        private ArrayNullableStringColumnWriter(
            ulong blockIndex, NativeFormatBlockWriter writer, int rows, byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _collected = new List<string?[]>(rows);
            _buffer = buffer;
            _index = 0;
            _encoded = false;
        }

        internal static ArrayNullableStringColumnWriter Create(NativeFormatBlockWriter writer, string columnName, int rows)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(Math.Max(1024, rows * 16));
            var blockIndex = writer.WriteColumnHeader(buffer, columnName, "Array(Nullable(String))", 0);
            return new ArrayNullableStringColumnWriter(blockIndex, writer, rows, buffer);
        }

        public ArrayNullableStringColumnWriter WriteNext(string?[] value)
        {
            if (_index >= _rows) throw new InvalidOperationException("No more rows to write.");
            _collected.Add(value);
            _index++;

            if (_index == _rows) EncodeIfNecessary();
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<string?[]> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var v in values) WriteNext(v);
            return _writer;
        }

        private void EncodeIfNecessary()
        {
            if (_encoded) return;

            var offsets = new long[_rows];
            long cumulative = 0;
            for (var i = 0; i < _rows; i++)
            {
                cumulative += _collected[i].Length;
                offsets[i] = cumulative;
            }
            var totalElements = (int)cumulative;

            var allValues = new string?[totalElements];
            var pos = 0;
            for (var i = 0; i < _rows; i++)
            {
                var arr = _collected[i];
                Array.Copy(arr, 0, allValues, pos, arr.Length);
                pos += arr.Length;
            }

            var offset = 0;

            // Offsets
            _buffer = _writer.EnsureCapacity(_blockIndex, offset, offset + _rows * 8);
            for (var i = 0; i < _rows; i++)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(_buffer.AsSpan(offset, 8), (ulong)offsets[i]);
                offset += 8;
            }

            // Inner null mask
            _buffer = _writer.EnsureCapacity(_blockIndex, offset, offset + totalElements);
            for (var i = 0; i < totalElements; i++)
            {
                _buffer[offset + i] = allValues[i] is null ? (byte)1 : (byte)0;
            }
            offset += totalElements;

            // Inner string values
            for (var i = 0; i < totalElements; i++)
            {
                var s = allValues[i] ?? string.Empty;
                var byteCount = Encoding.UTF8.GetByteCount(s);
                _buffer = _writer.EnsureCapacity(_blockIndex, offset,
                    offset + NativeFormatBlockWriter.MaxVarintLen64 + byteCount);
                offset += NativeFormatBlockWriter.WriteUtf8StringValue(_buffer.AsSpan(offset), s);
            }

            _encoded = true;
            _writer.SetDataLength(_blockIndex, offset);
        }
    }
    public ArrayNullableUInt32ColumnWriter CreateArrayNullableUInt32ColumnWriter(string columnName)
    {
        return ArrayNullableUInt32ColumnWriter.Create(this, columnName, checked((int)_rowsCount));
    }

    public ref struct ArrayNullableUInt32ColumnWriter : ISequentialColumnWriter<uint?[], ArrayNullableUInt32ColumnWriter>
    {
        private const int ValueSize = 4;
        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly List<uint?[]> _collected;
        private byte[] _buffer;
        private int _index;
        private bool _encoded;

        private ArrayNullableUInt32ColumnWriter(
            ulong blockIndex, NativeFormatBlockWriter writer, int rows, byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _collected = new List<uint?[]>(rows);
            _buffer = buffer;
            _index = 0;
            _encoded = false;
        }

        internal static ArrayNullableUInt32ColumnWriter Create(NativeFormatBlockWriter writer, string columnName, int rows)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(Math.Max(1024, rows * 16));
            var blockIndex = writer.WriteColumnHeader(buffer, columnName, "Array(Nullable(UInt32))", 0);
            return new ArrayNullableUInt32ColumnWriter(blockIndex, writer, rows, buffer);
        }

        public ArrayNullableUInt32ColumnWriter WriteNext(uint?[] value)
        {
            if (_index >= _rows) throw new InvalidOperationException("No more rows to write.");
            _collected.Add(value);
            _index++;

            if (_index == _rows) EncodeIfNecessary();
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<uint?[]> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var v in values) WriteNext(v);
            return _writer;
        }

        private void EncodeIfNecessary()
        {
            if (_encoded) return;

            var offsets = new long[_rows];
            long cumulative = 0;
            for (var i = 0; i < _rows; i++)
            {
                cumulative += _collected[i].Length;
                offsets[i] = cumulative;
            }
            var totalElements = (int)cumulative;

            var allValues = new uint?[totalElements];
            var pos = 0;
            for (var i = 0; i < _rows; i++)
            {
                var arr = _collected[i];
                Array.Copy(arr, 0, allValues, pos, arr.Length);
                pos += arr.Length;
            }

            var offset = 0;

            // Offsets
            _buffer = _writer.EnsureCapacity(_blockIndex, offset, offset + _rows * 8);
            for (var i = 0; i < _rows; i++)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(_buffer.AsSpan(offset, 8), (ulong)offsets[i]);
                offset += 8;
            }

            // Inner null mask
            _buffer = _writer.EnsureCapacity(_blockIndex, offset, offset + totalElements);
            for (var i = 0; i < totalElements; i++)
            {
                _buffer[offset + i] = allValues[i] is null ? (byte)1 : (byte)0;
            }
            offset += totalElements;

            // Inner fixed-size values
            _buffer = _writer.EnsureCapacity(_blockIndex, offset, offset + totalElements * ValueSize);
            for (var i = 0; i < totalElements; i++)
            {
                var dest = _buffer.AsSpan(offset, ValueSize);
                BinaryPrimitives.WriteUInt32LittleEndian(dest, allValues[i] ?? 0u);
                offset += ValueSize;
            }

            _encoded = true;
            _writer.SetDataLength(_blockIndex, offset);
        }
    }
}

#nullable enable
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;

namespace Clickhouse.Pure.Columns;

public partial class NativeFormatBlockReader
{
    public ArrayUInt16ColumnReader ReadArrayUInt16Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;

        if (!MatchesType(type, "Array(UInt16)"u8))
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected Array(UInt16) for column '{System.Text.Encoding.UTF8.GetString(name)}', but got '{System.Text.Encoding.UTF8.GetString(type)}'.");
        }

        return ArrayUInt16ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct ArrayUInt16ColumnReader : ISequentialColumnReader<ushort[]>
    {
        private const int ValueSize = 2;
        private readonly long[] _offsets;
        private readonly ushort[] _values;
        private readonly int _rows;
        private int _index;

        private ArrayUInt16ColumnReader(long[] offsets, ushort[] values, int rows)
        {
            _offsets = offsets;
            _values = values;
            _rows = rows;
            _index = 0;
        }

        public int Length => _rows;
        public bool HasMoreRows() => _index < _rows;

        public static ArrayUInt16ColumnReader CreateAndConsume(
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

            if (local + totalElements * ValueSize > data.Length) throw new IndexOutOfRangeException("array inner values out of range");
            var values = new ushort[totalElements];
            for (var i = 0; i < totalElements; i++)
            {
                values[i] = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(local, ValueSize));
                local += ValueSize;
            }

            offset = local;
            return new ArrayUInt16ColumnReader(offsets, values, rows);
        }

        public ushort[] ReadNext()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            var start = _index == 0 ? 0 : (int)_offsets[_index - 1];
            var end = (int)_offsets[_index];
            _index++;

            var result = new ushort[end - start];
            Array.Copy(_values, start, result, 0, result.Length);
            return result;
        }
    }
    public ArrayUInt32ColumnReader ReadArrayUInt32Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;

        if (!MatchesType(type, "Array(UInt32)"u8))
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected Array(UInt32) for column '{System.Text.Encoding.UTF8.GetString(name)}', but got '{System.Text.Encoding.UTF8.GetString(type)}'.");
        }

        return ArrayUInt32ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct ArrayUInt32ColumnReader : ISequentialColumnReader<uint[]>
    {
        private const int ValueSize = 4;
        private readonly long[] _offsets;
        private readonly uint[] _values;
        private readonly int _rows;
        private int _index;

        private ArrayUInt32ColumnReader(long[] offsets, uint[] values, int rows)
        {
            _offsets = offsets;
            _values = values;
            _rows = rows;
            _index = 0;
        }

        public int Length => _rows;
        public bool HasMoreRows() => _index < _rows;

        public static ArrayUInt32ColumnReader CreateAndConsume(
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

            if (local + totalElements * ValueSize > data.Length) throw new IndexOutOfRangeException("array inner values out of range");
            var values = new uint[totalElements];
            for (var i = 0; i < totalElements; i++)
            {
                values[i] = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(local, ValueSize));
                local += ValueSize;
            }

            offset = local;
            return new ArrayUInt32ColumnReader(offsets, values, rows);
        }

        public uint[] ReadNext()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            var start = _index == 0 ? 0 : (int)_offsets[_index - 1];
            var end = (int)_offsets[_index];
            _index++;

            var result = new uint[end - start];
            Array.Copy(_values, start, result, 0, result.Length);
            return result;
        }
    }
    public ArrayUInt64ColumnReader ReadArrayUInt64Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;

        if (!MatchesType(type, "Array(UInt64)"u8))
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected Array(UInt64) for column '{System.Text.Encoding.UTF8.GetString(name)}', but got '{System.Text.Encoding.UTF8.GetString(type)}'.");
        }

        return ArrayUInt64ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct ArrayUInt64ColumnReader : ISequentialColumnReader<ulong[]>
    {
        private const int ValueSize = 8;
        private readonly long[] _offsets;
        private readonly ulong[] _values;
        private readonly int _rows;
        private int _index;

        private ArrayUInt64ColumnReader(long[] offsets, ulong[] values, int rows)
        {
            _offsets = offsets;
            _values = values;
            _rows = rows;
            _index = 0;
        }

        public int Length => _rows;
        public bool HasMoreRows() => _index < _rows;

        public static ArrayUInt64ColumnReader CreateAndConsume(
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

            if (local + totalElements * ValueSize > data.Length) throw new IndexOutOfRangeException("array inner values out of range");
            var values = new ulong[totalElements];
            for (var i = 0; i < totalElements; i++)
            {
                values[i] = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(local, ValueSize));
                local += ValueSize;
            }

            offset = local;
            return new ArrayUInt64ColumnReader(offsets, values, rows);
        }

        public ulong[] ReadNext()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            var start = _index == 0 ? 0 : (int)_offsets[_index - 1];
            var end = (int)_offsets[_index];
            _index++;

            var result = new ulong[end - start];
            Array.Copy(_values, start, result, 0, result.Length);
            return result;
        }
    }
}

public partial class NativeFormatBlockWriter
{
    public ArrayUInt16ColumnWriter CreateArrayUInt16ColumnWriter(string columnName)
    {
        return ArrayUInt16ColumnWriter.Create(this, columnName, checked((int)_rowsCount));
    }

    public ref struct ArrayUInt16ColumnWriter : ISequentialColumnWriter<ushort[], ArrayUInt16ColumnWriter>
    {
        private const int ValueSize = 2;
        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly List<ushort[]> _collected;
        private byte[] _buffer;
        private int _index;
        private bool _encoded;

        private ArrayUInt16ColumnWriter(
            ulong blockIndex, NativeFormatBlockWriter writer, int rows, byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _collected = new List<ushort[]>(rows);
            _buffer = buffer;
            _index = 0;
            _encoded = false;
        }

        internal static ArrayUInt16ColumnWriter Create(NativeFormatBlockWriter writer, string columnName, int rows)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(Math.Max(1024, rows * 16));
            var blockIndex = writer.WriteColumnHeader(buffer, columnName, "Array(UInt16)", 0);
            return new ArrayUInt16ColumnWriter(blockIndex, writer, rows, buffer);
        }

        public ArrayUInt16ColumnWriter WriteNext(ushort[] value)
        {
            if (_index >= _rows) throw new InvalidOperationException("No more rows to write.");
            _collected.Add(value);
            _index++;

            if (_index == _rows) EncodeIfNecessary();
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<ushort[]> values)
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

            var allValues = new ushort[totalElements];
            var pos = 0;
            for (var i = 0; i < _rows; i++)
            {
                var arr = _collected[i];
                Array.Copy(arr, 0, allValues, pos, arr.Length);
                pos += arr.Length;
            }

            var offset = 0;

            _buffer = _writer.EnsureCapacity(_blockIndex, offset, offset + _rows * 8);
            for (var i = 0; i < _rows; i++)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(_buffer.AsSpan(offset, 8), (ulong)offsets[i]);
                offset += 8;
            }

            _buffer = _writer.EnsureCapacity(_blockIndex, offset, offset + totalElements * ValueSize);
            for (var i = 0; i < totalElements; i++)
            {
                var dest = _buffer.AsSpan(offset, ValueSize);
                BinaryPrimitives.WriteUInt16LittleEndian(dest, allValues[i]);
                offset += ValueSize;
            }

            _encoded = true;
            _writer.SetDataLength(_blockIndex, offset);
        }
    }
    public ArrayUInt32ColumnWriter CreateArrayUInt32ColumnWriter(string columnName)
    {
        return ArrayUInt32ColumnWriter.Create(this, columnName, checked((int)_rowsCount));
    }

    public ref struct ArrayUInt32ColumnWriter : ISequentialColumnWriter<uint[], ArrayUInt32ColumnWriter>
    {
        private const int ValueSize = 4;
        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly List<uint[]> _collected;
        private byte[] _buffer;
        private int _index;
        private bool _encoded;

        private ArrayUInt32ColumnWriter(
            ulong blockIndex, NativeFormatBlockWriter writer, int rows, byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _collected = new List<uint[]>(rows);
            _buffer = buffer;
            _index = 0;
            _encoded = false;
        }

        internal static ArrayUInt32ColumnWriter Create(NativeFormatBlockWriter writer, string columnName, int rows)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(Math.Max(1024, rows * 16));
            var blockIndex = writer.WriteColumnHeader(buffer, columnName, "Array(UInt32)", 0);
            return new ArrayUInt32ColumnWriter(blockIndex, writer, rows, buffer);
        }

        public ArrayUInt32ColumnWriter WriteNext(uint[] value)
        {
            if (_index >= _rows) throw new InvalidOperationException("No more rows to write.");
            _collected.Add(value);
            _index++;

            if (_index == _rows) EncodeIfNecessary();
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<uint[]> values)
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

            var allValues = new uint[totalElements];
            var pos = 0;
            for (var i = 0; i < _rows; i++)
            {
                var arr = _collected[i];
                Array.Copy(arr, 0, allValues, pos, arr.Length);
                pos += arr.Length;
            }

            var offset = 0;

            _buffer = _writer.EnsureCapacity(_blockIndex, offset, offset + _rows * 8);
            for (var i = 0; i < _rows; i++)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(_buffer.AsSpan(offset, 8), (ulong)offsets[i]);
                offset += 8;
            }

            _buffer = _writer.EnsureCapacity(_blockIndex, offset, offset + totalElements * ValueSize);
            for (var i = 0; i < totalElements; i++)
            {
                var dest = _buffer.AsSpan(offset, ValueSize);
                BinaryPrimitives.WriteUInt32LittleEndian(dest, allValues[i]);
                offset += ValueSize;
            }

            _encoded = true;
            _writer.SetDataLength(_blockIndex, offset);
        }
    }
    public ArrayUInt64ColumnWriter CreateArrayUInt64ColumnWriter(string columnName)
    {
        return ArrayUInt64ColumnWriter.Create(this, columnName, checked((int)_rowsCount));
    }

    public ref struct ArrayUInt64ColumnWriter : ISequentialColumnWriter<ulong[], ArrayUInt64ColumnWriter>
    {
        private const int ValueSize = 8;
        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly List<ulong[]> _collected;
        private byte[] _buffer;
        private int _index;
        private bool _encoded;

        private ArrayUInt64ColumnWriter(
            ulong blockIndex, NativeFormatBlockWriter writer, int rows, byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _collected = new List<ulong[]>(rows);
            _buffer = buffer;
            _index = 0;
            _encoded = false;
        }

        internal static ArrayUInt64ColumnWriter Create(NativeFormatBlockWriter writer, string columnName, int rows)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(Math.Max(1024, rows * 16));
            var blockIndex = writer.WriteColumnHeader(buffer, columnName, "Array(UInt64)", 0);
            return new ArrayUInt64ColumnWriter(blockIndex, writer, rows, buffer);
        }

        public ArrayUInt64ColumnWriter WriteNext(ulong[] value)
        {
            if (_index >= _rows) throw new InvalidOperationException("No more rows to write.");
            _collected.Add(value);
            _index++;

            if (_index == _rows) EncodeIfNecessary();
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<ulong[]> values)
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

            var allValues = new ulong[totalElements];
            var pos = 0;
            for (var i = 0; i < _rows; i++)
            {
                var arr = _collected[i];
                Array.Copy(arr, 0, allValues, pos, arr.Length);
                pos += arr.Length;
            }

            var offset = 0;

            _buffer = _writer.EnsureCapacity(_blockIndex, offset, offset + _rows * 8);
            for (var i = 0; i < _rows; i++)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(_buffer.AsSpan(offset, 8), (ulong)offsets[i]);
                offset += 8;
            }

            _buffer = _writer.EnsureCapacity(_blockIndex, offset, offset + totalElements * ValueSize);
            for (var i = 0; i < totalElements; i++)
            {
                var dest = _buffer.AsSpan(offset, ValueSize);
                BinaryPrimitives.WriteUInt64LittleEndian(dest, allValues[i]);
                offset += ValueSize;
            }

            _encoded = true;
            _writer.SetDataLength(_blockIndex, offset);
        }
    }
}

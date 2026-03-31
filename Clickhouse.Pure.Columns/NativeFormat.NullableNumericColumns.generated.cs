#nullable enable
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;
using System.Collections.Generic;

namespace Clickhouse.Pure.Columns;

public partial class NativeFormatBlockReader
{
    public NullableUInt16ColumnReader ReadNullableUInt16Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;

        if (!MatchesType(type, "Nullable(UInt16)"u8))
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected Nullable(UInt16) for column '{Encoding.UTF8.GetString(name)}', but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return NullableUInt16ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct NullableUInt16ColumnReader : ISequentialColumnReader<ushort?>
    {
        private const int ValueSize = 2;
        private readonly ReadOnlySpan<byte> _nullsMask;
        private readonly ReadOnlySpan<byte> _data;
        private int _valuesOffset;
        private readonly int _rows;
        private int _index;

        private NullableUInt16ColumnReader(
            ReadOnlySpan<byte> nullsMask, ReadOnlySpan<byte> data, int valuesOffset, int rows)
        {
            _nullsMask = nullsMask;
            _data = data;
            _valuesOffset = valuesOffset;
            _rows = rows;
            _index = 0;
        }

        public int Length => _rows;
        public bool HasMoreRows() => _index < _rows;

        public static NullableUInt16ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data, scoped ref int offset, int rows)
        {
            if (offset + rows > data.Length) throw new IndexOutOfRangeException("nullable nulls out of range");
            var mask = data.Slice(offset, rows);
            var valuesStart = offset + rows;
            var total = (long)rows * ValueSize;
            if (valuesStart + total > data.Length) throw new IndexOutOfRangeException("nullable values out of range");
            offset = valuesStart + (int)total;
            return new NullableUInt16ColumnReader(mask, data, valuesStart, rows);
        }

        public ushort? ReadNext()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            var isNull = _nullsMask[_index] != 0;
            var v = BinaryPrimitives.ReadUInt16LittleEndian(_data.Slice(_valuesOffset, ValueSize));
            _valuesOffset += ValueSize;
            _index++;
            return isNull ? null : v;
        }
    }
    public NullableUInt32ColumnReader ReadNullableUInt32Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;

        if (!MatchesType(type, "Nullable(UInt32)"u8))
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected Nullable(UInt32) for column '{Encoding.UTF8.GetString(name)}', but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return NullableUInt32ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct NullableUInt32ColumnReader : ISequentialColumnReader<uint?>
    {
        private const int ValueSize = 4;
        private readonly ReadOnlySpan<byte> _nullsMask;
        private readonly ReadOnlySpan<byte> _data;
        private int _valuesOffset;
        private readonly int _rows;
        private int _index;

        private NullableUInt32ColumnReader(
            ReadOnlySpan<byte> nullsMask, ReadOnlySpan<byte> data, int valuesOffset, int rows)
        {
            _nullsMask = nullsMask;
            _data = data;
            _valuesOffset = valuesOffset;
            _rows = rows;
            _index = 0;
        }

        public int Length => _rows;
        public bool HasMoreRows() => _index < _rows;

        public static NullableUInt32ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data, scoped ref int offset, int rows)
        {
            if (offset + rows > data.Length) throw new IndexOutOfRangeException("nullable nulls out of range");
            var mask = data.Slice(offset, rows);
            var valuesStart = offset + rows;
            var total = (long)rows * ValueSize;
            if (valuesStart + total > data.Length) throw new IndexOutOfRangeException("nullable values out of range");
            offset = valuesStart + (int)total;
            return new NullableUInt32ColumnReader(mask, data, valuesStart, rows);
        }

        public uint? ReadNext()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            var isNull = _nullsMask[_index] != 0;
            var v = BinaryPrimitives.ReadUInt32LittleEndian(_data.Slice(_valuesOffset, ValueSize));
            _valuesOffset += ValueSize;
            _index++;
            return isNull ? null : v;
        }
    }
    public NullableUInt64ColumnReader ReadNullableUInt64Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;

        if (!MatchesType(type, "Nullable(UInt64)"u8))
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected Nullable(UInt64) for column '{Encoding.UTF8.GetString(name)}', but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return NullableUInt64ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct NullableUInt64ColumnReader : ISequentialColumnReader<ulong?>
    {
        private const int ValueSize = 8;
        private readonly ReadOnlySpan<byte> _nullsMask;
        private readonly ReadOnlySpan<byte> _data;
        private int _valuesOffset;
        private readonly int _rows;
        private int _index;

        private NullableUInt64ColumnReader(
            ReadOnlySpan<byte> nullsMask, ReadOnlySpan<byte> data, int valuesOffset, int rows)
        {
            _nullsMask = nullsMask;
            _data = data;
            _valuesOffset = valuesOffset;
            _rows = rows;
            _index = 0;
        }

        public int Length => _rows;
        public bool HasMoreRows() => _index < _rows;

        public static NullableUInt64ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data, scoped ref int offset, int rows)
        {
            if (offset + rows > data.Length) throw new IndexOutOfRangeException("nullable nulls out of range");
            var mask = data.Slice(offset, rows);
            var valuesStart = offset + rows;
            var total = (long)rows * ValueSize;
            if (valuesStart + total > data.Length) throw new IndexOutOfRangeException("nullable values out of range");
            offset = valuesStart + (int)total;
            return new NullableUInt64ColumnReader(mask, data, valuesStart, rows);
        }

        public ulong? ReadNext()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            var isNull = _nullsMask[_index] != 0;
            var v = BinaryPrimitives.ReadUInt64LittleEndian(_data.Slice(_valuesOffset, ValueSize));
            _valuesOffset += ValueSize;
            _index++;
            return isNull ? null : v;
        }
    }
    public NullableBoolColumnReader ReadNullableBoolColumn()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;

        if (!MatchesType(type, "Nullable(Bool)"u8))
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected Nullable(Bool) for column '{Encoding.UTF8.GetString(name)}', but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return NullableBoolColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct NullableBoolColumnReader : ISequentialColumnReader<bool?>
    {
        private const int ValueSize = 1;
        private readonly ReadOnlySpan<byte> _nullsMask;
        private readonly ReadOnlySpan<byte> _data;
        private int _valuesOffset;
        private readonly int _rows;
        private int _index;

        private NullableBoolColumnReader(
            ReadOnlySpan<byte> nullsMask, ReadOnlySpan<byte> data, int valuesOffset, int rows)
        {
            _nullsMask = nullsMask;
            _data = data;
            _valuesOffset = valuesOffset;
            _rows = rows;
            _index = 0;
        }

        public int Length => _rows;
        public bool HasMoreRows() => _index < _rows;

        public static NullableBoolColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data, scoped ref int offset, int rows)
        {
            if (offset + rows > data.Length) throw new IndexOutOfRangeException("nullable nulls out of range");
            var mask = data.Slice(offset, rows);
            var valuesStart = offset + rows;
            var total = (long)rows * ValueSize;
            if (valuesStart + total > data.Length) throw new IndexOutOfRangeException("nullable values out of range");
            offset = valuesStart + (int)total;
            return new NullableBoolColumnReader(mask, data, valuesStart, rows);
        }

        public bool? ReadNext()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            var isNull = _nullsMask[_index] != 0;
            var v = MemoryMarshal.Read<bool>(_data.Slice(_valuesOffset, ValueSize));
            _valuesOffset += ValueSize;
            _index++;
            return isNull ? null : v;
        }
    }
}

public partial class NativeFormatBlockWriter
{
    public NullableUInt16ColumnWriter CreateNullableUInt16ColumnWriter(string columnName)
    {
        return NullableUInt16ColumnWriter.Create(this, columnName, checked((int)_rowsCount));
    }

    public ref struct NullableUInt16ColumnWriter : ISequentialColumnWriter<ushort?, NullableUInt16ColumnWriter>
    {
        private const int ValueSize = 2;
        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly int _maskLength;
        private readonly byte[] _buffer;
        private int _index;

        private NullableUInt16ColumnWriter(
            ulong blockIndex, NativeFormatBlockWriter writer, int rows, byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _maskLength = rows;
            _buffer = buffer;
            _index = 0;
            _buffer.AsSpan(0, _maskLength + rows * ValueSize).Clear();
            _writer.SetDataLength(_blockIndex, _maskLength);
        }

        internal static NullableUInt16ColumnWriter Create(
            NativeFormatBlockWriter writer, string columnName, int rows)
        {
            var totalSize = rows + rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(buffer, columnName, "Nullable(UInt16)", 0);
            return new NullableUInt16ColumnWriter(blockIndex, writer, rows, buffer);
        }

        public NullableUInt16ColumnWriter WriteNext(ushort? value)
        {
            if (_index >= _rows) throw new InvalidOperationException("No more rows to write.");
            if (value is null)
            {
                _buffer[_index] = 1;
            }
            else
            {
                _buffer[_index] = 0;
                var dest = _buffer.AsSpan(_maskLength + _index * ValueSize, ValueSize);
                BinaryPrimitives.WriteUInt16LittleEndian(dest, value.Value);
            }
            _index++;
            _writer.SetDataLength(_blockIndex, _maskLength + _index * ValueSize);
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<ushort?> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var v in values) WriteNext(v);
            return _writer;
        }
    }
    public NullableUInt32ColumnWriter CreateNullableUInt32ColumnWriter(string columnName)
    {
        return NullableUInt32ColumnWriter.Create(this, columnName, checked((int)_rowsCount));
    }

    public ref struct NullableUInt32ColumnWriter : ISequentialColumnWriter<uint?, NullableUInt32ColumnWriter>
    {
        private const int ValueSize = 4;
        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly int _maskLength;
        private readonly byte[] _buffer;
        private int _index;

        private NullableUInt32ColumnWriter(
            ulong blockIndex, NativeFormatBlockWriter writer, int rows, byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _maskLength = rows;
            _buffer = buffer;
            _index = 0;
            _buffer.AsSpan(0, _maskLength + rows * ValueSize).Clear();
            _writer.SetDataLength(_blockIndex, _maskLength);
        }

        internal static NullableUInt32ColumnWriter Create(
            NativeFormatBlockWriter writer, string columnName, int rows)
        {
            var totalSize = rows + rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(buffer, columnName, "Nullable(UInt32)", 0);
            return new NullableUInt32ColumnWriter(blockIndex, writer, rows, buffer);
        }

        public NullableUInt32ColumnWriter WriteNext(uint? value)
        {
            if (_index >= _rows) throw new InvalidOperationException("No more rows to write.");
            if (value is null)
            {
                _buffer[_index] = 1;
            }
            else
            {
                _buffer[_index] = 0;
                var dest = _buffer.AsSpan(_maskLength + _index * ValueSize, ValueSize);
                BinaryPrimitives.WriteUInt32LittleEndian(dest, value.Value);
            }
            _index++;
            _writer.SetDataLength(_blockIndex, _maskLength + _index * ValueSize);
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<uint?> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var v in values) WriteNext(v);
            return _writer;
        }
    }
    public NullableUInt64ColumnWriter CreateNullableUInt64ColumnWriter(string columnName)
    {
        return NullableUInt64ColumnWriter.Create(this, columnName, checked((int)_rowsCount));
    }

    public ref struct NullableUInt64ColumnWriter : ISequentialColumnWriter<ulong?, NullableUInt64ColumnWriter>
    {
        private const int ValueSize = 8;
        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly int _maskLength;
        private readonly byte[] _buffer;
        private int _index;

        private NullableUInt64ColumnWriter(
            ulong blockIndex, NativeFormatBlockWriter writer, int rows, byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _maskLength = rows;
            _buffer = buffer;
            _index = 0;
            _buffer.AsSpan(0, _maskLength + rows * ValueSize).Clear();
            _writer.SetDataLength(_blockIndex, _maskLength);
        }

        internal static NullableUInt64ColumnWriter Create(
            NativeFormatBlockWriter writer, string columnName, int rows)
        {
            var totalSize = rows + rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(buffer, columnName, "Nullable(UInt64)", 0);
            return new NullableUInt64ColumnWriter(blockIndex, writer, rows, buffer);
        }

        public NullableUInt64ColumnWriter WriteNext(ulong? value)
        {
            if (_index >= _rows) throw new InvalidOperationException("No more rows to write.");
            if (value is null)
            {
                _buffer[_index] = 1;
            }
            else
            {
                _buffer[_index] = 0;
                var dest = _buffer.AsSpan(_maskLength + _index * ValueSize, ValueSize);
                BinaryPrimitives.WriteUInt64LittleEndian(dest, value.Value);
            }
            _index++;
            _writer.SetDataLength(_blockIndex, _maskLength + _index * ValueSize);
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<ulong?> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var v in values) WriteNext(v);
            return _writer;
        }
    }
    public NullableBoolColumnWriter CreateNullableBoolColumnWriter(string columnName)
    {
        return NullableBoolColumnWriter.Create(this, columnName, checked((int)_rowsCount));
    }

    public ref struct NullableBoolColumnWriter : ISequentialColumnWriter<bool?, NullableBoolColumnWriter>
    {
        private const int ValueSize = 1;
        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly int _maskLength;
        private readonly byte[] _buffer;
        private int _index;

        private NullableBoolColumnWriter(
            ulong blockIndex, NativeFormatBlockWriter writer, int rows, byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _maskLength = rows;
            _buffer = buffer;
            _index = 0;
            _buffer.AsSpan(0, _maskLength + rows * ValueSize).Clear();
            _writer.SetDataLength(_blockIndex, _maskLength);
        }

        internal static NullableBoolColumnWriter Create(
            NativeFormatBlockWriter writer, string columnName, int rows)
        {
            var totalSize = rows + rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(buffer, columnName, "Nullable(Bool)", 0);
            return new NullableBoolColumnWriter(blockIndex, writer, rows, buffer);
        }

        public NullableBoolColumnWriter WriteNext(bool? value)
        {
            if (_index >= _rows) throw new InvalidOperationException("No more rows to write.");
            if (value is null)
            {
                _buffer[_index] = 1;
            }
            else
            {
                _buffer[_index] = 0;
                var dest = _buffer.AsSpan(_maskLength + _index * ValueSize, ValueSize);
                dest[0] = value.Value ? (byte)1 : (byte)0;
            }
            _index++;
            _writer.SetDataLength(_blockIndex, _maskLength + _index * ValueSize);
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<bool?> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var v in values) WriteNext(v);
            return _writer;
        }
    }
}

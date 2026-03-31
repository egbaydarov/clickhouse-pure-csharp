#nullable enable
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;

namespace Clickhouse.Pure.Columns;

public partial class NativeFormatBlockReader
{
    public NullableDecimal128ColumnReader ReadNullableDecimal128Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;

        // Strip "Nullable(" prefix and ")" suffix to get inner decimal type
        if (type.Length < 11
            || !type[..9].SequenceEqual("Nullable("u8)
            || type[^1] != (byte)')')
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected Nullable(Decimal...) for column '{Encoding.UTF8.GetString(name)}', but got '{Encoding.UTF8.GetString(type)}'.");
        }

        var innerType = type[9..^1];
        var descriptor = DecimalTypeParser.Parse(innerType, name, 128);

        return NullableDecimal128ColumnReader.CreateAndConsume(
            _buffer.Span, ref _offset, (int)_rowsCount, descriptor.Scale, descriptor.Precision);
    }

    public ref struct NullableDecimal128ColumnReader : ISequentialColumnReader<Decimal128Value?>
    {
        private const int ValueSize = 16;
        private readonly ReadOnlySpan<byte> _nullsMask;
        private readonly ReadOnlySpan<byte> _data;
        private int _valuesOffset;
        private readonly int _rows;
        private int _index;
        private readonly int _scale;
        private readonly int _precision;

        private NullableDecimal128ColumnReader(
            ReadOnlySpan<byte> nullsMask, ReadOnlySpan<byte> data, int valuesOffset,
            int rows, int scale, int precision)
        {
            _nullsMask = nullsMask;
            _data = data;
            _valuesOffset = valuesOffset;
            _rows = rows;
            _scale = scale;
            _precision = precision;
            _index = 0;
        }

        public int Length => _rows;
        public int Scale => _scale;
        public int Precision => _precision;
        public bool HasMoreRows() => _index < _rows;

        public static NullableDecimal128ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data, scoped ref int offset, int rows, int scale, int precision)
        {
            if (offset + rows > data.Length) throw new IndexOutOfRangeException("nullable nulls out of range");
            var mask = data.Slice(offset, rows);
            var valuesStart = offset + rows;
            var total = (long)rows * ValueSize;
            if (valuesStart + total > data.Length) throw new IndexOutOfRangeException("nullable Decimal128 values out of range");
            offset = valuesStart + (int)total;
            return new NullableDecimal128ColumnReader(mask, data, valuesStart, rows, scale, precision);
        }

        public Decimal128Value? ReadNext()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            var isNull = _nullsMask[_index] != 0;
            _index++;

            var raw = BinaryPrimitives.ReadInt128LittleEndian(_data.Slice(_valuesOffset, ValueSize));
            _valuesOffset += ValueSize;

            return isNull ? null : Decimal128Value.FromUnscaled(raw, _scale);
        }
    }
}

public partial class NativeFormatBlockWriter
{
    public NullableDecimal128ColumnWriter CreateNullableDecimal128ColumnWriter(
        string columnName, int scale, int? precision = null)
    {
        var actualPrecision = precision ?? 38;
        DecimalTypeParser.ValidatePrecision(columnName, "Decimal128", actualPrecision, 19, 38);
        DecimalTypeParser.ValidateScaleForPrecision(columnName, "Decimal128", scale, actualPrecision);

        var typeName = precision is null
            ? $"Nullable(Decimal128({scale}))"
            : $"Nullable(Decimal({actualPrecision}, {scale}))";

        return NullableDecimal128ColumnWriter.Create(this, columnName, typeName, checked((int)_rowsCount), scale, actualPrecision);
    }

    public ref struct NullableDecimal128ColumnWriter : ISequentialColumnWriter<Decimal128Value?, NullableDecimal128ColumnWriter>
    {
        private const int ValueSize = 16;
        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly int _maskLength;
        private readonly byte[] _buffer;
        private readonly int _scale;
        private readonly int _precision;
        private int _index;

        private NullableDecimal128ColumnWriter(
            ulong blockIndex, NativeFormatBlockWriter writer, int rows, byte[] buffer, int scale, int precision)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _maskLength = rows;
            _buffer = buffer;
            _scale = scale;
            _precision = precision;
            _index = 0;
            _buffer.AsSpan(0, _maskLength + rows * ValueSize).Clear();
            _writer.SetDataLength(_blockIndex, _maskLength);
        }

        internal static NullableDecimal128ColumnWriter Create(
            NativeFormatBlockWriter writer, string columnName, string typeName, int rows, int scale, int precision)
        {
            var totalSize = rows + rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(buffer, columnName, typeName, 0);
            return new NullableDecimal128ColumnWriter(blockIndex, writer, rows, buffer, scale, precision);
        }

        public NullableDecimal128ColumnWriter WriteNext(Decimal128Value? value)
        {
            if (_index >= _rows) throw new InvalidOperationException("No more rows to write.");

            if (value is null)
            {
                _buffer[_index] = 1;
            }
            else
            {
                _buffer[_index] = 0;
                var normalized = value.Value.WithScale(_scale);
                DecimalMath.EnsureFitsPrecision(normalized.UnscaledValue, _precision);
                var dest = _buffer.AsSpan(_maskLength + _index * ValueSize, ValueSize);
                BinaryPrimitives.WriteInt128LittleEndian(dest, normalized.UnscaledValue);
            }

            _index++;
            _writer.SetDataLength(_blockIndex, _maskLength + _index * ValueSize);
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<Decimal128Value?> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var v in values) WriteNext(v);
            return _writer;
        }
    }
}

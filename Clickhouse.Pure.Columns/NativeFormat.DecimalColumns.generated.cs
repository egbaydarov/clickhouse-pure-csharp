#nullable enable
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Numerics;

namespace Clickhouse.Pure.Columns;

public partial class NativeFormatBlockReader
{
    public Decimal32ColumnReader ReadDecimal32Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        var descriptor = DecimalTypeParser.Parse(type, name, 32);

        return Decimal32ColumnReader.CreateAndConsume(
            _buffer.Span,
            ref _offset,
            (int)_rowsCount,
            descriptor.Scale,
            descriptor.Precision);
    }

    public ref struct Decimal32ColumnReader : ISequentialColumnReader<decimal>
    {
        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private readonly int _rows;
        private int _index;
        private readonly int _scale;
        private readonly int _precision;

        private Decimal32ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows,
            int scale,
            int precision)
        {
            _data = data;
            _offset = startOffset;
            _rows = rows;
            _scale = scale;
            _precision = precision;
            _index = 0;
        }

        public static Decimal32ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows,
            int scale,
            int precision)
        {
            var start = offset;
            var total = (long)rows * 4;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Decimal column out of range");
            }

            offset = start + (int)total;

            return new Decimal32ColumnReader(data, start, rows, scale, precision);
        }

        public int Length => _rows;

        public int Scale => _scale;

        public int Precision => _precision;

        public bool HasMoreRows() => _index < _rows;


        public decimal ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 4 > _data.Length)
            {
                throw new IndexOutOfRangeException("Decimal value out of range");
            }


            var raw = BinaryPrimitives.ReadInt32LittleEndian(_data.Slice(_offset, 4));

            _offset += 4;

            return DecimalMath.FromInt32(raw, _scale);

        }

    }
    public Decimal64ColumnReader ReadDecimal64Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        var descriptor = DecimalTypeParser.Parse(type, name, 64);

        return Decimal64ColumnReader.CreateAndConsume(
            _buffer.Span,
            ref _offset,
            (int)_rowsCount,
            descriptor.Scale,
            descriptor.Precision);
    }

    public ref struct Decimal64ColumnReader : ISequentialColumnReader<decimal>
    {
        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private readonly int _rows;
        private int _index;
        private readonly int _scale;
        private readonly int _precision;

        private Decimal64ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows,
            int scale,
            int precision)
        {
            _data = data;
            _offset = startOffset;
            _rows = rows;
            _scale = scale;
            _precision = precision;
            _index = 0;
        }

        public static Decimal64ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows,
            int scale,
            int precision)
        {
            var start = offset;
            var total = (long)rows * 8;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Decimal column out of range");
            }

            offset = start + (int)total;

            return new Decimal64ColumnReader(data, start, rows, scale, precision);
        }

        public int Length => _rows;

        public int Scale => _scale;

        public int Precision => _precision;

        public bool HasMoreRows() => _index < _rows;


        public decimal ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 8 > _data.Length)
            {
                throw new IndexOutOfRangeException("Decimal value out of range");
            }


            var raw = BinaryPrimitives.ReadInt64LittleEndian(_data.Slice(_offset, 8));

            _offset += 8;

            return DecimalMath.FromInt64(raw, _scale);

        }

    }
    public Decimal128ColumnReader ReadDecimal128Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        var descriptor = DecimalTypeParser.Parse(type, name, 128);

        return Decimal128ColumnReader.CreateAndConsume(
            _buffer.Span,
            ref _offset,
            (int)_rowsCount,
            descriptor.Scale,
            descriptor.Precision);
    }

    public ref struct Decimal128ColumnReader : ISequentialColumnReader<Decimal128Value>
    {
        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private readonly int _rows;
        private int _index;
        private readonly int _scale;
        private readonly int _precision;

        private Decimal128ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows,
            int scale,
            int precision)
        {
            _data = data;
            _offset = startOffset;
            _rows = rows;
            _scale = scale;
            _precision = precision;
            _index = 0;
        }

        public static Decimal128ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows,
            int scale,
            int precision)
        {
            var start = offset;
            var total = (long)rows * 16;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Decimal column out of range");
            }

            offset = start + (int)total;

            return new Decimal128ColumnReader(data, start, rows, scale, precision);
        }

        public int Length => _rows;

        public int Scale => _scale;

        public int Precision => _precision;

        public bool HasMoreRows() => _index < _rows;


        public Decimal128Value ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 16 > _data.Length)
            {
                throw new IndexOutOfRangeException("Decimal value out of range");
            }

            var raw = BinaryPrimitives.ReadInt128LittleEndian(_data.Slice(_offset, 16));
            _offset += 16;
            return Decimal128Value.FromUnscaled(raw, _scale);
        }

    }
    public Decimal256ColumnReader ReadDecimal256Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        var descriptor = DecimalTypeParser.Parse(type, name, 256);

        return Decimal256ColumnReader.CreateAndConsume(
            _buffer.Span,
            ref _offset,
            (int)_rowsCount,
            descriptor.Scale,
            descriptor.Precision);
    }

    public ref struct Decimal256ColumnReader : ISequentialColumnReader<Decimal256Value>
    {
        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private readonly int _rows;
        private int _index;
        private readonly int _scale;
        private readonly int _precision;

        private Decimal256ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows,
            int scale,
            int precision)
        {
            _data = data;
            _offset = startOffset;
            _rows = rows;
            _scale = scale;
            _precision = precision;
            _index = 0;
        }

        public static Decimal256ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows,
            int scale,
            int precision)
        {
            var start = offset;
            var total = (long)rows * 32;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Decimal column out of range");
            }

            offset = start + (int)total;

            return new Decimal256ColumnReader(data, start, rows, scale, precision);
        }

        public int Length => _rows;

        public int Scale => _scale;

        public int Precision => _precision;

        public bool HasMoreRows() => _index < _rows;


        public Decimal256Value ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 32 > _data.Length)
            {
                throw new IndexOutOfRangeException("Decimal value out of range");
            }

            var raw = DecimalMath.ReadInt256LittleEndian(_data.Slice(_offset, 32));
            _offset += 32;
            return Decimal256Value.FromUnscaled(raw, _scale);
        }

    }

}

public partial class NativeFormatBlockWriter
{
    public Decimal32ColumnWriter CreateDecimal32ColumnWriter(
        string columnName,
        int scale, int? precision = null)
    {

        var actualPrecision = precision ?? 9;
        DecimalTypeParser.ValidatePrecision(columnName, "Decimal32", actualPrecision, 1, 9);
        DecimalTypeParser.ValidateScaleForPrecision(columnName, "Decimal32", scale, actualPrecision);
        var typeName = precision is null
            ? $"Decimal32({scale})"
            : $"Decimal({actualPrecision}, {scale})";
        WriteColumnHeader(columnName, typeName);
        return Decimal32ColumnWriter.Create(this, checked((int)_rowsCount), scale, actualPrecision);

    }

    public ref struct Decimal32ColumnWriter : ISequentialColumnWriter<decimal, Decimal32ColumnWriter>
    {
        private const int ValueSize = 4;

        private NativeFormatBlockWriter _writer;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private readonly int _scale;
        private readonly int _precision;
        private int _index;
        private bool _segmentAdded;

        private Decimal32ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer,
            int scale,
            int precision)
        {
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _scale = scale;
            _precision = precision;
            _index = 0;
            _segmentAdded = false;
        }

        internal static Decimal32ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows,
            int scale,
            int precision)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            return new Decimal32ColumnWriter(writer, rows, buffer, scale, precision);
        }

        public int Length => _rows;

        public int Scale => _scale;

        public int Precision => _precision;

        public Decimal32ColumnWriter WriteNext(decimal value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);


            var rawValue = DecimalMath.ScaleDecimalToInt32(value, _scale, _precision);
            BinaryPrimitives.WriteInt32LittleEndian(dest, rawValue);


            _index++;

            if (_index == _rows)
            {
                EnsureSegmentAdded();
            }

            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<decimal> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            if (_index == _rows)
            {
                EnsureSegmentAdded();
            }

            return _writer;
        }

        public ReadOnlyMemory<byte> GetColumnData()
        {
            if (_index != _rows)
            {
                throw new InvalidOperationException("Attempted to get column data before all rows were written.");
            }

            EnsureSegmentAdded();
            return new ReadOnlyMemory<byte>(_buffer, 0, _rows * ValueSize);
        }

        private void EnsureSegmentAdded()
        {
            if (_segmentAdded)
            {
                return;
            }

            var segment = new ReadOnlyMemory<byte>(_buffer, 0, _rows * ValueSize);
            _writer.AddSegment(segment, _buffer);
            _segmentAdded = true;
        }
    }
    public Decimal64ColumnWriter CreateDecimal64ColumnWriter(
        string columnName,
        int scale, int? precision = null)
    {

        var actualPrecision = precision ?? 18;
        DecimalTypeParser.ValidatePrecision(columnName, "Decimal64", actualPrecision, 10, 18);
        DecimalTypeParser.ValidateScaleForPrecision(columnName, "Decimal64", scale, actualPrecision);
        var typeName = precision is null
            ? $"Decimal64({scale})"
            : $"Decimal({actualPrecision}, {scale})";
        WriteColumnHeader(columnName, typeName);
        return Decimal64ColumnWriter.Create(this, checked((int)_rowsCount), scale, actualPrecision);

    }

    public ref struct Decimal64ColumnWriter : ISequentialColumnWriter<decimal, Decimal64ColumnWriter>
    {
        private const int ValueSize = 8;

        private NativeFormatBlockWriter _writer;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private readonly int _scale;
        private readonly int _precision;
        private int _index;
        private bool _segmentAdded;

        private Decimal64ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer,
            int scale,
            int precision)
        {
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _scale = scale;
            _precision = precision;
            _index = 0;
            _segmentAdded = false;
        }

        internal static Decimal64ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows,
            int scale,
            int precision)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            return new Decimal64ColumnWriter(writer, rows, buffer, scale, precision);
        }

        public int Length => _rows;

        public int Scale => _scale;

        public int Precision => _precision;

        public Decimal64ColumnWriter WriteNext(decimal value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);


            var rawValue = DecimalMath.ScaleDecimalToInt64(value, _scale, _precision);
            BinaryPrimitives.WriteInt64LittleEndian(dest, rawValue);


            _index++;

            if (_index == _rows)
            {
                EnsureSegmentAdded();
            }

            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<decimal> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            if (_index == _rows)
            {
                EnsureSegmentAdded();
            }

            return _writer;
        }

        public ReadOnlyMemory<byte> GetColumnData()
        {
            if (_index != _rows)
            {
                throw new InvalidOperationException("Attempted to get column data before all rows were written.");
            }

            EnsureSegmentAdded();
            return new ReadOnlyMemory<byte>(_buffer, 0, _rows * ValueSize);
        }

        private void EnsureSegmentAdded()
        {
            if (_segmentAdded)
            {
                return;
            }

            var segment = new ReadOnlyMemory<byte>(_buffer, 0, _rows * ValueSize);
            _writer.AddSegment(segment, _buffer);
            _segmentAdded = true;
        }
    }
    public Decimal128ColumnWriter CreateDecimal128ColumnWriter(
        string columnName,
        int scale, int? precision = null)
    {

        var actualPrecision = precision ?? 38;
        DecimalTypeParser.ValidatePrecision(columnName, "Decimal128", actualPrecision, 19, 38);
        DecimalTypeParser.ValidateScaleForPrecision(columnName, "Decimal128", scale, actualPrecision);
        var typeName = precision is null
            ? $"Decimal128({scale})"
            : $"Decimal({actualPrecision}, {scale})";
        WriteColumnHeader(columnName, typeName);
        return Decimal128ColumnWriter.Create(this, checked((int)_rowsCount), scale, actualPrecision);

    }

    public ref struct Decimal128ColumnWriter : ISequentialColumnWriter<Decimal128Value, Decimal128ColumnWriter>
    {
        private const int ValueSize = 16;

        private NativeFormatBlockWriter _writer;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private readonly int _scale;
        private readonly int _precision;
        private int _index;
        private bool _segmentAdded;

        private Decimal128ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer,
            int scale,
            int precision)
        {
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _scale = scale;
            _precision = precision;
            _index = 0;
            _segmentAdded = false;
        }

        internal static Decimal128ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows,
            int scale,
            int precision)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            return new Decimal128ColumnWriter(writer, rows, buffer, scale, precision);
        }

        public int Length => _rows;

        public int Scale => _scale;

        public int Precision => _precision;

        public Decimal128ColumnWriter WriteNext(Decimal128Value value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);

            var normalized = value.WithScale(_scale);
            DecimalMath.EnsureFitsPrecision(normalized.UnscaledValue, _precision);
            BinaryPrimitives.WriteInt128LittleEndian(dest, normalized.UnscaledValue);

            _index++;

            if (_index == _rows)
            {
                EnsureSegmentAdded();
            }

            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<Decimal128Value> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            if (_index == _rows)
            {
                EnsureSegmentAdded();
            }

            return _writer;
        }

        public ReadOnlyMemory<byte> GetColumnData()
        {
            if (_index != _rows)
            {
                throw new InvalidOperationException("Attempted to get column data before all rows were written.");
            }

            EnsureSegmentAdded();
            return new ReadOnlyMemory<byte>(_buffer, 0, _rows * ValueSize);
        }

        private void EnsureSegmentAdded()
        {
            if (_segmentAdded)
            {
                return;
            }

            var segment = new ReadOnlyMemory<byte>(_buffer, 0, _rows * ValueSize);
            _writer.AddSegment(segment, _buffer);
            _segmentAdded = true;
        }
    }
    public Decimal256ColumnWriter CreateDecimal256ColumnWriter(
        string columnName,
        int scale, int? precision = null)
    {

        var actualPrecision = precision ?? 76;
        DecimalTypeParser.ValidatePrecision(columnName, "Decimal256", actualPrecision, 39, 76);
        DecimalTypeParser.ValidateScaleForPrecision(columnName, "Decimal256", scale, actualPrecision);
        var typeName = precision is null
            ? $"Decimal256({scale})"
            : $"Decimal({actualPrecision}, {scale})";
        WriteColumnHeader(columnName, typeName);
        return Decimal256ColumnWriter.Create(this, checked((int)_rowsCount), scale, actualPrecision);

    }

    public ref struct Decimal256ColumnWriter : ISequentialColumnWriter<Decimal256Value, Decimal256ColumnWriter>
    {
        private const int ValueSize = 32;

        private NativeFormatBlockWriter _writer;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private readonly int _scale;
        private readonly int _precision;
        private int _index;
        private bool _segmentAdded;

        private Decimal256ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer,
            int scale,
            int precision)
        {
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _scale = scale;
            _precision = precision;
            _index = 0;
            _segmentAdded = false;
        }

        internal static Decimal256ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows,
            int scale,
            int precision)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            return new Decimal256ColumnWriter(writer, rows, buffer, scale, precision);
        }

        public int Length => _rows;

        public int Scale => _scale;

        public int Precision => _precision;

        public Decimal256ColumnWriter WriteNext(Decimal256Value value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);

            var normalized = value.WithScale(_scale);
            DecimalMath.EnsureFitsPrecision(normalized.UnscaledValue, _precision);
            DecimalMath.WriteInt256LittleEndian(dest, normalized.UnscaledValue);

            _index++;

            if (_index == _rows)
            {
                EnsureSegmentAdded();
            }

            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<Decimal256Value> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            if (_index == _rows)
            {
                EnsureSegmentAdded();
            }

            return _writer;
        }

        public ReadOnlyMemory<byte> GetColumnData()
        {
            if (_index != _rows)
            {
                throw new InvalidOperationException("Attempted to get column data before all rows were written.");
            }

            EnsureSegmentAdded();
            return new ReadOnlyMemory<byte>(_buffer, 0, _rows * ValueSize);
        }

        private void EnsureSegmentAdded()
        {
            if (_segmentAdded)
            {
                return;
            }

            var segment = new ReadOnlyMemory<byte>(_buffer, 0, _rows * ValueSize);
            _writer.AddSegment(segment, _buffer);
            _segmentAdded = true;
        }
    }

}


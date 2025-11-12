#nullable enable
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Collections.Generic;

namespace Clickhouse.Pure.Columns;

public partial class NativeFormatBlockReader
{
    public Date32ColumnReader ReadDate32Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "Date32"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected Date32 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return Date32ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct Date32ColumnReader : ISequentialColumnReader<DateOnly>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private Date32ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static Date32ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 4;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new Date32ColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public DateOnly ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 4 > _data.Length)
            {
                throw new IndexOutOfRangeException("Date32 value out of range");
            }

            var v = DateOnlyExt.From1970_01_01DaysInt32(_data.Slice(_offset, 4));
            _offset += 4;
            return v;
        }
    }
    public DateColumnReader ReadDateColumn()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "Date"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected Date for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return DateColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct DateColumnReader : ISequentialColumnReader<DateOnly>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private DateColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static DateColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 2;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new DateColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public DateOnly ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 2 > _data.Length)
            {
                throw new IndexOutOfRangeException("Date value out of range");
            }

            var v = DateOnlyExt.From1970_01_01Days(_data.Slice(_offset, 2));
            _offset += 2;
            return v;
        }
    }
    public IPv4ColumnReader ReadIPv4Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "IPv4"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected IPv4 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return IPv4ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct IPv4ColumnReader : ISequentialColumnReader<IPAddress>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private IPv4ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static IPv4ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 4;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new IPv4ColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public IPAddress ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 4 > _data.Length)
            {
                throw new IndexOutOfRangeException("IPv4 value out of range");
            }

            var v = IpAddressExt.FromLittleEndianIPv4(_data.Slice(_offset, 4));
            _offset += 4;
            return v;
        }
    }
    public BoolColumnReader ReadBoolColumn()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "Bool"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected Bool for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return BoolColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct BoolColumnReader : ISequentialColumnReader<bool>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private BoolColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static BoolColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 1;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new BoolColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public bool ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 1 > _data.Length)
            {
                throw new IndexOutOfRangeException("Bool value out of range");
            }

            var v = MemoryMarshal.Read<bool>(_data.Slice(_offset, 1));
            _offset += 1;
            return v;
        }
    }
    public UInt8ColumnReader ReadUInt8Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "UInt8"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected UInt8 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return UInt8ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct UInt8ColumnReader : ISequentialColumnReader<byte>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private UInt8ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static UInt8ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 1;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new UInt8ColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public byte ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 1 > _data.Length)
            {
                throw new IndexOutOfRangeException("UInt8 value out of range");
            }

            var v = MemoryMarshal.Read<byte>(_data.Slice(_offset, 1));
            _offset += 1;
            return v;
        }
    }
    public UInt16ColumnReader ReadUInt16Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "UInt16"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected UInt16 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return UInt16ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct UInt16ColumnReader : ISequentialColumnReader<ushort>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private UInt16ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static UInt16ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 2;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new UInt16ColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public ushort ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 2 > _data.Length)
            {
                throw new IndexOutOfRangeException("UInt16 value out of range");
            }

            var v = BinaryPrimitives.ReadUInt16LittleEndian(_data.Slice(_offset, 2));
            _offset += 2;
            return v;
        }
    }
    public UInt32ColumnReader ReadUInt32Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "UInt32"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected UInt32 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return UInt32ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct UInt32ColumnReader : ISequentialColumnReader<uint>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private UInt32ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static UInt32ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 4;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new UInt32ColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public uint ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 4 > _data.Length)
            {
                throw new IndexOutOfRangeException("UInt32 value out of range");
            }

            var v = BinaryPrimitives.ReadUInt32LittleEndian(_data.Slice(_offset, 4));
            _offset += 4;
            return v;
        }
    }
    public UInt64ColumnReader ReadUInt64Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "UInt64"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected UInt64 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return UInt64ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct UInt64ColumnReader : ISequentialColumnReader<ulong>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private UInt64ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static UInt64ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 8;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new UInt64ColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public ulong ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 8 > _data.Length)
            {
                throw new IndexOutOfRangeException("UInt64 value out of range");
            }

            var v = BinaryPrimitives.ReadUInt64LittleEndian(_data.Slice(_offset, 8));
            _offset += 8;
            return v;
        }
    }
    public UInt128ColumnReader ReadUInt128Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "UInt128"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected UInt128 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return UInt128ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct UInt128ColumnReader : ISequentialColumnReader<UInt128>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private UInt128ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static UInt128ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 16;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new UInt128ColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public UInt128 ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 16 > _data.Length)
            {
                throw new IndexOutOfRangeException("UInt128 value out of range");
            }

            var v = BinaryPrimitives.ReadUInt128LittleEndian(_data.Slice(_offset, 16));
            _offset += 16;
            return v;
        }
    }
    public Int8ColumnReader ReadInt8Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "Int8"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected Int8 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return Int8ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct Int8ColumnReader : ISequentialColumnReader<sbyte>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private Int8ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static Int8ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 1;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new Int8ColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public sbyte ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 1 > _data.Length)
            {
                throw new IndexOutOfRangeException("Int8 value out of range");
            }

            var v = MemoryMarshal.Read<sbyte>(_data.Slice(_offset, 1));
            _offset += 1;
            return v;
        }
    }
    public Int16ColumnReader ReadInt16Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "Int16"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected Int16 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return Int16ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct Int16ColumnReader : ISequentialColumnReader<short>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private Int16ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static Int16ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 2;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new Int16ColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public short ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 2 > _data.Length)
            {
                throw new IndexOutOfRangeException("Int16 value out of range");
            }

            var v = BinaryPrimitives.ReadInt16LittleEndian(_data.Slice(_offset, 2));
            _offset += 2;
            return v;
        }
    }
    public Int32ColumnReader ReadInt32Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "Int32"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected Int32 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return Int32ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct Int32ColumnReader : ISequentialColumnReader<int>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private Int32ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static Int32ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 4;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new Int32ColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public int ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 4 > _data.Length)
            {
                throw new IndexOutOfRangeException("Int32 value out of range");
            }

            var v = BinaryPrimitives.ReadInt32LittleEndian(_data.Slice(_offset, 4));
            _offset += 4;
            return v;
        }
    }
    public Int64ColumnReader ReadInt64Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "Int64"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected Int64 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return Int64ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct Int64ColumnReader : ISequentialColumnReader<long>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private Int64ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static Int64ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 8;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new Int64ColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public long ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 8 > _data.Length)
            {
                throw new IndexOutOfRangeException("Int64 value out of range");
            }

            var v = BinaryPrimitives.ReadInt64LittleEndian(_data.Slice(_offset, 8));
            _offset += 8;
            return v;
        }
    }
    public Int128ColumnReader ReadInt128Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "Int128"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected Int128 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return Int128ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct Int128ColumnReader : ISequentialColumnReader<Int128>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private Int128ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static Int128ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 16;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new Int128ColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public Int128 ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 16 > _data.Length)
            {
                throw new IndexOutOfRangeException("Int128 value out of range");
            }

            var v = BinaryPrimitives.ReadInt128LittleEndian(_data.Slice(_offset, 16));
            _offset += 16;
            return v;
        }
    }
    public Float32ColumnReader ReadFloat32Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "Float32"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected Float32 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return Float32ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct Float32ColumnReader : ISequentialColumnReader<float>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private Float32ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static Float32ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 4;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new Float32ColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public float ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 4 > _data.Length)
            {
                throw new IndexOutOfRangeException("Float32 value out of range");
            }

            var v = BinaryPrimitives.ReadSingleLittleEndian(_data.Slice(_offset, 4));
            _offset += 4;
            return v;
        }
    }
    public Float64ColumnReader ReadFloat64Column()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }
 
        // header
        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;
        if (!MatchesType(type, "Float64"u8))
        {
            throw new InvalidOperationException($"Column type mismatch. Expected Float64 for column {Encoding.UTF8.GetString(name)}, but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return Float64ColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct Float64ColumnReader : ISequentialColumnReader<double>
    {
        private readonly int _rows;

        private readonly ReadOnlySpan<byte> _data;
        private int _offset;
        private int _index;

        private Float64ColumnReader(
            ReadOnlySpan<byte> data,
            int startOffset,
            int rows)
        {
            _rows = rows;
            _data = data;
            _offset = startOffset;

            _index = 0;
        }

        public static Float64ColumnReader CreateAndConsume(
            ReadOnlySpan<byte> data,
            scoped ref int offset,
            int rows)
        {
            var start = offset;
            var total = (long)rows * 8;

            if (total < 0 || start + total > data.Length)
            {
                throw new IndexOutOfRangeException("Float64 column out of range");
            }

            // advance offset by ref
            offset = start + (int)total;

            return new Float64ColumnReader(data, start, rows);
        }

        public int Length => _rows;

        public bool HasMoreRows() => _index < _rows;

        public double ReadNext()
        {
            if (_index >= _rows)
            {
                throw new IndexOutOfRangeException("No more values");
            }

            _index++;
            if (_offset + 8 > _data.Length)
            {
                throw new IndexOutOfRangeException("Float64 value out of range");
            }

            var v = BinaryPrimitives.ReadDoubleLittleEndian(_data.Slice(_offset, 8));
            _offset += 8;
            return v;
        }
    }
}

public partial class NativeFormatBlockWriter
{
    public Date32ColumnWriter CreateDate32ColumnWriter(string columnName)
    {
        return Date32ColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct Date32ColumnWriter : ISequentialColumnWriter<DateOnly, Date32ColumnWriter>
    {
        private const int ValueSize = 4;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private Date32ColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public Date32ColumnWriter WriteNext(DateOnly value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            var days = value.DayNumber - 719162;
            BinaryPrimitives.WriteInt32LittleEndian(dest, days);
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<DateOnly> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static Date32ColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "Date32",
                dataLength: totalSize);

            return new Date32ColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public DateColumnWriter CreateDateColumnWriter(string columnName)
    {
        return DateColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct DateColumnWriter : ISequentialColumnWriter<DateOnly, DateColumnWriter>
    {
        private const int ValueSize = 2;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private DateColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public DateColumnWriter WriteNext(DateOnly value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            var days = value.DayNumber - 719162;
            if ((uint)days > ushort.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "DateOnly is outside of ClickHouse Date range (1970-01-01..2106-02-07).");
            }
            BinaryPrimitives.WriteUInt16LittleEndian(dest, (ushort)days);
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<DateOnly> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static DateColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "Date",
                dataLength: totalSize);

            return new DateColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public IPv4ColumnWriter CreateIPv4ColumnWriter(string columnName)
    {
        return IPv4ColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct IPv4ColumnWriter : ISequentialColumnWriter<IPAddress, IPv4ColumnWriter>
    {
        private const int ValueSize = 4;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private IPv4ColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public IPv4ColumnWriter WriteNext(IPAddress value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            IpAddressExt.WriteLittleEndianIPv4(dest, value);
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<IPAddress> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static IPv4ColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "IPv4",
                dataLength: totalSize);

            return new IPv4ColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public BoolColumnWriter CreateBoolColumnWriter(string columnName)
    {
        return BoolColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct BoolColumnWriter : ISequentialColumnWriter<bool, BoolColumnWriter>
    {
        private const int ValueSize = 1;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private BoolColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public BoolColumnWriter WriteNext(bool value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            dest[0] = value ? (byte)1 : (byte)0;
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<bool> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static BoolColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "Bool",
                dataLength: totalSize);

            return new BoolColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public UInt8ColumnWriter CreateUInt8ColumnWriter(string columnName)
    {
        return UInt8ColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct UInt8ColumnWriter : ISequentialColumnWriter<byte, UInt8ColumnWriter>
    {
        private const int ValueSize = 1;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private UInt8ColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public UInt8ColumnWriter WriteNext(byte value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            dest[0] = value;
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<byte> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static UInt8ColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "UInt8",
                dataLength: totalSize);

            return new UInt8ColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public UInt16ColumnWriter CreateUInt16ColumnWriter(string columnName)
    {
        return UInt16ColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct UInt16ColumnWriter : ISequentialColumnWriter<ushort, UInt16ColumnWriter>
    {
        private const int ValueSize = 2;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private UInt16ColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public UInt16ColumnWriter WriteNext(ushort value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            BinaryPrimitives.WriteUInt16LittleEndian(dest, value);
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<ushort> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static UInt16ColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "UInt16",
                dataLength: totalSize);

            return new UInt16ColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public UInt32ColumnWriter CreateUInt32ColumnWriter(string columnName)
    {
        return UInt32ColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct UInt32ColumnWriter : ISequentialColumnWriter<uint, UInt32ColumnWriter>
    {
        private const int ValueSize = 4;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private UInt32ColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public UInt32ColumnWriter WriteNext(uint value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            BinaryPrimitives.WriteUInt32LittleEndian(dest, value);
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<uint> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static UInt32ColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "UInt32",
                dataLength: totalSize);

            return new UInt32ColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public UInt64ColumnWriter CreateUInt64ColumnWriter(string columnName)
    {
        return UInt64ColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct UInt64ColumnWriter : ISequentialColumnWriter<ulong, UInt64ColumnWriter>
    {
        private const int ValueSize = 8;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private UInt64ColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public UInt64ColumnWriter WriteNext(ulong value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            BinaryPrimitives.WriteUInt64LittleEndian(dest, value);
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<ulong> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static UInt64ColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "UInt64",
                dataLength: totalSize);

            return new UInt64ColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public UInt128ColumnWriter CreateUInt128ColumnWriter(string columnName)
    {
        return UInt128ColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct UInt128ColumnWriter : ISequentialColumnWriter<UInt128, UInt128ColumnWriter>
    {
        private const int ValueSize = 16;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private UInt128ColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public UInt128ColumnWriter WriteNext(UInt128 value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            BinaryPrimitives.WriteUInt128LittleEndian(dest, value);
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<UInt128> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static UInt128ColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "UInt128",
                dataLength: totalSize);

            return new UInt128ColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public Int8ColumnWriter CreateInt8ColumnWriter(string columnName)
    {
        return Int8ColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct Int8ColumnWriter : ISequentialColumnWriter<sbyte, Int8ColumnWriter>
    {
        private const int ValueSize = 1;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private Int8ColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public Int8ColumnWriter WriteNext(sbyte value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            dest[0] = unchecked((byte)value);
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<sbyte> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static Int8ColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "Int8",
                dataLength: totalSize);

            return new Int8ColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public Int16ColumnWriter CreateInt16ColumnWriter(string columnName)
    {
        return Int16ColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct Int16ColumnWriter : ISequentialColumnWriter<short, Int16ColumnWriter>
    {
        private const int ValueSize = 2;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private Int16ColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public Int16ColumnWriter WriteNext(short value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            BinaryPrimitives.WriteInt16LittleEndian(dest, value);
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<short> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static Int16ColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "Int16",
                dataLength: totalSize);

            return new Int16ColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public Int32ColumnWriter CreateInt32ColumnWriter(string columnName)
    {
        return Int32ColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct Int32ColumnWriter : ISequentialColumnWriter<int, Int32ColumnWriter>
    {
        private const int ValueSize = 4;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private Int32ColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public Int32ColumnWriter WriteNext(int value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            BinaryPrimitives.WriteInt32LittleEndian(dest, value);
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<int> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static Int32ColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "Int32",
                dataLength: totalSize);

            return new Int32ColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public Int64ColumnWriter CreateInt64ColumnWriter(string columnName)
    {
        return Int64ColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct Int64ColumnWriter : ISequentialColumnWriter<long, Int64ColumnWriter>
    {
        private const int ValueSize = 8;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private Int64ColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public Int64ColumnWriter WriteNext(long value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            BinaryPrimitives.WriteInt64LittleEndian(dest, value);
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<long> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static Int64ColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "Int64",
                dataLength: totalSize);

            return new Int64ColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public Int128ColumnWriter CreateInt128ColumnWriter(string columnName)
    {
        return Int128ColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct Int128ColumnWriter : ISequentialColumnWriter<Int128, Int128ColumnWriter>
    {
        private const int ValueSize = 16;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private Int128ColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public Int128ColumnWriter WriteNext(Int128 value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            BinaryPrimitives.WriteInt128LittleEndian(dest, value);
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<Int128> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static Int128ColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "Int128",
                dataLength: totalSize);

            return new Int128ColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public Float32ColumnWriter CreateFloat32ColumnWriter(string columnName)
    {
        return Float32ColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct Float32ColumnWriter : ISequentialColumnWriter<float, Float32ColumnWriter>
    {
        private const int ValueSize = 4;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private Float32ColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public Float32ColumnWriter WriteNext(float value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            BinaryPrimitives.WriteSingleLittleEndian(dest, value);
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<float> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static Float32ColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "Float32",
                dataLength: totalSize);

            return new Float32ColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
    public Float64ColumnWriter CreateFloat64ColumnWriter(string columnName)
    {
        return Float64ColumnWriter.Create(
            writer: this,
            columnName: columnName,
            rows: checked((int)_rowsCount));
    }

    public ref struct Float64ColumnWriter : ISequentialColumnWriter<double, Float64ColumnWriter>
    {
        private const int ValueSize = 8;

        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly byte[] _buffer;
        private int _index;

        private Float64ColumnWriter(
            ulong blockIndex,
            NativeFormatBlockWriter writer,
            int rows,
            byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _buffer = buffer;
            _index = 0;
        }

        public Float64ColumnWriter WriteNext(double value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var dest = _buffer.AsSpan(_index * ValueSize, ValueSize);
            BinaryPrimitives.WriteDoubleLittleEndian(dest, value);
            _index++;

            _writer.SetDataLength(_blockIndex, checked(_index * ValueSize));
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<double> values)
        {
            if (values is null) throw new ArgumentNullException(nameof(values));
            foreach (var value in values)
            {
                WriteNext(value);
            }

            return _writer;
        }

        internal static Float64ColumnWriter Create(
            NativeFormatBlockWriter writer,
            string columnName,
            int rows)
        {
            var totalSize = rows * ValueSize;
            var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            var blockIndex = writer.WriteColumnHeader(
                buffer: buffer,
                columnName: columnName,
                typeName: "Float64",
                dataLength: totalSize);

            return new Float64ColumnWriter(
                blockIndex: blockIndex,
                writer: writer,
                rows: rows,
                buffer: buffer);
        }
    }
}
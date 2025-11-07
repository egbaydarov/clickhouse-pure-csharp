#nullable enable
using System;
using System.Buffers.Binary;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Collections.Generic;

namespace Clickhouse.Pure.ColumnCodeGenerator;

public partial class NativeFormatBlockReader
{
    public Date32ColumnReader AdvanceDate32Column()
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

        return Date32ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static Date32ColumnReader CreateAndAdvance(
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

        public DateOnly GetCellValueAndAdvance()
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
    public DateColumnReader AdvanceDateColumn()
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

        return DateColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static DateColumnReader CreateAndAdvance(
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

        public DateOnly GetCellValueAndAdvance()
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
    public IPv4ColumnReader AdvanceIPv4Column()
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

        return IPv4ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static IPv4ColumnReader CreateAndAdvance(
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

        public IPAddress GetCellValueAndAdvance()
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

            var v = IPAddressExt.FromLittleEndianIPv4(_data.Slice(_offset, 4));
            _offset += 4;
            return v;
        }
    }
    public BoolColumnReader AdvanceBoolColumn()
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

        return BoolColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static BoolColumnReader CreateAndAdvance(
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

        public bool GetCellValueAndAdvance()
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
    public UInt8ColumnReader AdvanceUInt8Column()
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

        return UInt8ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static UInt8ColumnReader CreateAndAdvance(
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

        public byte GetCellValueAndAdvance()
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
    public UInt16ColumnReader AdvanceUInt16Column()
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

        return UInt16ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static UInt16ColumnReader CreateAndAdvance(
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

        public ushort GetCellValueAndAdvance()
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
    public UInt32ColumnReader AdvanceUInt32Column()
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

        return UInt32ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static UInt32ColumnReader CreateAndAdvance(
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

        public uint GetCellValueAndAdvance()
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
    public UInt64ColumnReader AdvanceUInt64Column()
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

        return UInt64ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static UInt64ColumnReader CreateAndAdvance(
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

        public ulong GetCellValueAndAdvance()
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
    public UInt128ColumnReader AdvanceUInt128Column()
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

        return UInt128ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static UInt128ColumnReader CreateAndAdvance(
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

        public UInt128 GetCellValueAndAdvance()
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
    public Int8ColumnReader AdvanceInt8Column()
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

        return Int8ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static Int8ColumnReader CreateAndAdvance(
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

        public sbyte GetCellValueAndAdvance()
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
    public Int16ColumnReader AdvanceInt16Column()
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

        return Int16ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static Int16ColumnReader CreateAndAdvance(
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

        public short GetCellValueAndAdvance()
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
    public Int32ColumnReader AdvanceInt32Column()
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

        return Int32ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static Int32ColumnReader CreateAndAdvance(
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

        public int GetCellValueAndAdvance()
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
    public Int64ColumnReader AdvanceInt64Column()
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

        return Int64ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static Int64ColumnReader CreateAndAdvance(
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

        public long GetCellValueAndAdvance()
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
    public Int128ColumnReader AdvanceInt128Column()
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

        return Int128ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static Int128ColumnReader CreateAndAdvance(
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

        public Int128 GetCellValueAndAdvance()
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
    public Float32ColumnReader AdvanceFloat32Column()
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

        return Float32ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static Float32ColumnReader CreateAndAdvance(
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

        public float GetCellValueAndAdvance()
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
    public Float64ColumnReader AdvanceFloat64Column()
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

        return Float64ColumnReader.CreateAndAdvance(_buffer.Span, ref _offset, (int)_rowsCount);
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

        public static Float64ColumnReader CreateAndAdvance(
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

        public double GetCellValueAndAdvance()
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
    public Date32ColumnWriter AdvanceDate32ColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "Date32");
        return Date32ColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct Date32ColumnWriter : ISequentialColumnWriter<DateOnly>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private Date32ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static Date32ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 4);
            return new Date32ColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(DateOnly value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 4;
            var dest = _writer.GetWritableSpan(
                destStart,
                4);
            var days = value.DayNumber - 719162;
            BinaryPrimitives.WriteInt32LittleEndian(dest, days);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<DateOnly> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 4);
        }
    }
    public DateColumnWriter AdvanceDateColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "Date");
        return DateColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct DateColumnWriter : ISequentialColumnWriter<DateOnly>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private DateColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static DateColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 2);
            return new DateColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(DateOnly value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 2;
            var dest = _writer.GetWritableSpan(
                destStart,
                2);
            var days = value.DayNumber - 719162;
            if ((uint)days > ushort.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "DateOnly is outside of ClickHouse Date range (1970-01-01..2106-02-07).");
            }
            BinaryPrimitives.WriteUInt16LittleEndian(dest, (ushort)days);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<DateOnly> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 2);
        }
    }
    public IPv4ColumnWriter AdvanceIPv4ColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "IPv4");
        return IPv4ColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct IPv4ColumnWriter : ISequentialColumnWriter<IPAddress>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private IPv4ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static IPv4ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 4);
            return new IPv4ColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(IPAddress value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 4;
            var dest = _writer.GetWritableSpan(
                destStart,
                4);
            IPAddressExt.WriteLittleEndianIPv4(dest, value);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<IPAddress> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 4);
        }
    }
    public BoolColumnWriter AdvanceBoolColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "Bool");
        return BoolColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct BoolColumnWriter : ISequentialColumnWriter<bool>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private BoolColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static BoolColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 1);
            return new BoolColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(bool value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 1;
            var dest = _writer.GetWritableSpan(
                destStart,
                1);
            dest[0] = value ? (byte)1 : (byte)0;
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<bool> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 1);
        }
    }
    public UInt8ColumnWriter AdvanceUInt8ColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "UInt8");
        return UInt8ColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct UInt8ColumnWriter : ISequentialColumnWriter<byte>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private UInt8ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static UInt8ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 1);
            return new UInt8ColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(byte value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 1;
            var dest = _writer.GetWritableSpan(
                destStart,
                1);
            dest[0] = value;
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<byte> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 1);
        }
    }
    public UInt16ColumnWriter AdvanceUInt16ColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "UInt16");
        return UInt16ColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct UInt16ColumnWriter : ISequentialColumnWriter<ushort>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private UInt16ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static UInt16ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 2);
            return new UInt16ColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(ushort value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 2;
            var dest = _writer.GetWritableSpan(
                destStart,
                2);
            BinaryPrimitives.WriteUInt16LittleEndian(dest, value);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<ushort> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 2);
        }
    }
    public UInt32ColumnWriter AdvanceUInt32ColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "UInt32");
        return UInt32ColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct UInt32ColumnWriter : ISequentialColumnWriter<uint>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private UInt32ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static UInt32ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 4);
            return new UInt32ColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(uint value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 4;
            var dest = _writer.GetWritableSpan(
                destStart,
                4);
            BinaryPrimitives.WriteUInt32LittleEndian(dest, value);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<uint> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 4);
        }
    }
    public UInt64ColumnWriter AdvanceUInt64ColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "UInt64");
        return UInt64ColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct UInt64ColumnWriter : ISequentialColumnWriter<ulong>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private UInt64ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static UInt64ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 8);
            return new UInt64ColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(ulong value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 8;
            var dest = _writer.GetWritableSpan(
                destStart,
                8);
            BinaryPrimitives.WriteUInt64LittleEndian(dest, value);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<ulong> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 8);
        }
    }
    public UInt128ColumnWriter AdvanceUInt128ColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "UInt128");
        return UInt128ColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct UInt128ColumnWriter : ISequentialColumnWriter<UInt128>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private UInt128ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static UInt128ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 16);
            return new UInt128ColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(UInt128 value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 16;
            var dest = _writer.GetWritableSpan(
                destStart,
                16);
            BinaryPrimitives.WriteUInt128LittleEndian(dest, value);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<UInt128> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 16);
        }
    }
    public Int8ColumnWriter AdvanceInt8ColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "Int8");
        return Int8ColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct Int8ColumnWriter : ISequentialColumnWriter<sbyte>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private Int8ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static Int8ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 1);
            return new Int8ColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(sbyte value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 1;
            var dest = _writer.GetWritableSpan(
                destStart,
                1);
            dest[0] = unchecked((byte)value);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<sbyte> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 1);
        }
    }
    public Int16ColumnWriter AdvanceInt16ColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "Int16");
        return Int16ColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct Int16ColumnWriter : ISequentialColumnWriter<short>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private Int16ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static Int16ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 2);
            return new Int16ColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(short value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 2;
            var dest = _writer.GetWritableSpan(
                destStart,
                2);
            BinaryPrimitives.WriteInt16LittleEndian(dest, value);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<short> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 2);
        }
    }
    public Int32ColumnWriter AdvanceInt32ColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "Int32");
        return Int32ColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct Int32ColumnWriter : ISequentialColumnWriter<int>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private Int32ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static Int32ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 4);
            return new Int32ColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(int value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 4;
            var dest = _writer.GetWritableSpan(
                destStart,
                4);
            BinaryPrimitives.WriteInt32LittleEndian(dest, value);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<int> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 4);
        }
    }
    public Int64ColumnWriter AdvanceInt64ColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "Int64");
        return Int64ColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct Int64ColumnWriter : ISequentialColumnWriter<long>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private Int64ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static Int64ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 8);
            return new Int64ColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(long value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 8;
            var dest = _writer.GetWritableSpan(
                destStart,
                8);
            BinaryPrimitives.WriteInt64LittleEndian(dest, value);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<long> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 8);
        }
    }
    public Int128ColumnWriter AdvanceInt128ColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "Int128");
        return Int128ColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct Int128ColumnWriter : ISequentialColumnWriter<Int128>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private Int128ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static Int128ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 16);
            return new Int128ColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(Int128 value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 16;
            var dest = _writer.GetWritableSpan(
                destStart,
                16);
            BinaryPrimitives.WriteInt128LittleEndian(dest, value);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<Int128> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 16);
        }
    }
    public Float32ColumnWriter AdvanceFloat32ColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "Float32");
        return Float32ColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct Float32ColumnWriter : ISequentialColumnWriter<float>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private Float32ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static Float32ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 4);
            return new Float32ColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(float value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 4;
            var dest = _writer.GetWritableSpan(
                destStart,
                4);
            BinaryPrimitives.WriteSingleLittleEndian(dest, value);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<float> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 4);
        }
    }
    public Float64ColumnWriter AdvanceFloat64ColumnWriter(string columnName)
    {
        WriteColumnHeader(columnName, "Float64");
        return Float64ColumnWriter.Create(this, checked((int)_rowsCount));
    }

    public ref struct Float64ColumnWriter : ISequentialColumnWriter<double>
    {
        private NativeFormatBlockWriter _writer;
        private readonly int _startOffset;
        private readonly int _rows;
        private int _index;

        private Float64ColumnWriter(
            NativeFormatBlockWriter writer,
            int rows,
            int startOffset)
        {
            _writer = writer;
            _rows = rows;
            _startOffset = startOffset;
            _index = 0;
        }

        internal static Float64ColumnWriter Create(
            NativeFormatBlockWriter writer,
            int rows)
        {
            var startOffset = writer.ReserveFixedSizeColumn(rows, 8);
            return new Float64ColumnWriter(writer, rows, startOffset);
        }

        public int Length => _rows;

        public void WriteCellValueAndAdvance(double value)
        {
            if (_index >= _rows)
            {
                throw new InvalidOperationException("No more rows to write.");
            }

            var destStart = _startOffset + _index * 8;
            var dest = _writer.GetWritableSpan(
                destStart,
                8);
            BinaryPrimitives.WriteDoubleLittleEndian(dest, value);
            _index++;
        }

        public void WriteCellValuesAndAdvance(IEnumerable<double> values)
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

            return _writer.GetColumnSlice(_startOffset, _rows * 8);
        }
    }
}
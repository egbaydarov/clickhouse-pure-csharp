#nullable enable
using System.Buffers.Binary;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

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

            var v = DateOnlyExt.From1900_01_01Days(_data.Slice(_offset, 4));
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

            var v = new IPAddress(_data.Slice(_offset, 4));
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
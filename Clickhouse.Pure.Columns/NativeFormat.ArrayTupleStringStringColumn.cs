#nullable enable
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;

namespace Clickhouse.Pure.Columns;

public partial class NativeFormatBlockReader
{
    /// <summary>
    /// Reads an <c>Array(Tuple(String, String))</c> column. Named tuple elements
    /// (e.g. <c>Array(Tuple(Language String, Text String))</c>) are accepted as well.
    /// </summary>
    public ArrayTupleStringStringColumnReader ReadArrayTupleStringStringColumn()
    {
        if (_columnsRead >= _columnsCount)
        {
            throw new InvalidOperationException("No more columns available in this block.");
        }

        var name = ReadHeaderString();
        var type = ReadHeaderString();
        _columnsRead++;

        if (!ArrayTupleStringStringTypeName.Matches(type))
        {
            throw new InvalidOperationException(
                $"Column type mismatch. Expected Array(Tuple(String, String)) for column '{Encoding.UTF8.GetString(name)}', but got '{Encoding.UTF8.GetString(type)}'.");
        }

        return ArrayTupleStringStringColumnReader.CreateAndConsume(_buffer.Span, ref _offset, (int)_rowsCount);
    }

    public ref struct ArrayTupleStringStringColumnReader : ISequentialColumnReader<(string, string)[]>
    {
        private readonly long[] _offsets;
        private readonly (string, string)[] _values;
        private readonly int _rows;
        private int _index;

        private ArrayTupleStringStringColumnReader(long[] offsets, (string, string)[] values, int rows)
        {
            _offsets = offsets;
            _values = values;
            _rows = rows;
            _index = 0;
        }

        public int Length => _rows;
        public bool HasMoreRows() => _index < _rows;

        public static ArrayTupleStringStringColumnReader CreateAndConsume(
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
            var values = new (string, string)[totalElements];

            // Tuple is serialized element-wise: first all Item1 strings, then all Item2 strings.
            for (var i = 0; i < totalElements; i++)
            {
                values[i].Item1 = ReadString(ref local, data);
            }

            for (var i = 0; i < totalElements; i++)
            {
                values[i].Item2 = ReadString(ref local, data);
            }

            offset = local;
            return new ArrayTupleStringStringColumnReader(offsets, values, rows);
        }

        private static string ReadString(ref int local, ReadOnlySpan<byte> data)
        {
            var len = (int)ReadUVarInt(ref local, data);
            if (local + len > data.Length) throw new IndexOutOfRangeException("array inner string out of range");
            var s = Encoding.UTF8.GetString(data.Slice(local, len));
            local += len;
            return s;
        }

        public (string, string)[] ReadNext()
        {
            if (_index >= _rows) throw new IndexOutOfRangeException("no more values");
            var start = _index == 0 ? 0 : (int)_offsets[_index - 1];
            var end = (int)_offsets[_index];
            _index++;

            var result = new (string, string)[end - start];
            Array.Copy(_values, start, result, 0, result.Length);
            return result;
        }
    }
}

public partial class NativeFormatBlockWriter
{
    /// <summary>
    /// Creates a writer for an <c>Array(Tuple(String, String))</c> column.
    /// When <paramref name="firstElementName"/> and <paramref name="secondElementName"/> are provided,
    /// a named tuple type is declared (e.g. <c>Array(Tuple(Language String, Text String))</c>).
    /// </summary>
    public ArrayTupleStringStringColumnWriter CreateArrayTupleStringStringColumnWriter(
        string columnName,
        string? firstElementName = null,
        string? secondElementName = null)
    {
        var typeName = ArrayTupleStringStringTypeName.Build(firstElementName, secondElementName);
        return ArrayTupleStringStringColumnWriter.Create(this, columnName, typeName, checked((int)_rowsCount));
    }

    public ref struct ArrayTupleStringStringColumnWriter : ISequentialColumnWriter<(string, string)[], ArrayTupleStringStringColumnWriter>
    {
        private NativeFormatBlockWriter _writer;
        private readonly ulong _blockIndex;
        private readonly int _rows;
        private readonly List<(string, string)[]> _collected;
        private byte[] _buffer;
        private int _index;
        private bool _encoded;

        private ArrayTupleStringStringColumnWriter(
            ulong blockIndex, NativeFormatBlockWriter writer, int rows, byte[] buffer)
        {
            _blockIndex = blockIndex;
            _writer = writer;
            _rows = rows;
            _collected = new List<(string, string)[]>(rows);
            _buffer = buffer;
            _index = 0;
            _encoded = false;
        }

        internal static ArrayTupleStringStringColumnWriter Create(
            NativeFormatBlockWriter writer, string columnName, string typeName, int rows)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(Math.Max(1024, rows * 16));
            var blockIndex = writer.WriteColumnHeader(buffer, columnName, typeName, 0);
            return new ArrayTupleStringStringColumnWriter(blockIndex, writer, rows, buffer);
        }

        public ArrayTupleStringStringColumnWriter WriteNext((string, string)[] value)
        {
            if (_index >= _rows) throw new InvalidOperationException("No more rows to write.");
            ArgumentNullException.ThrowIfNull(value);
            _collected.Add(value);
            _index++;

            if (_index == _rows) EncodeIfNecessary();
            return this;
        }

        public NativeFormatBlockWriter WriteAll(IEnumerable<(string, string)[]> values)
        {
            ArgumentNullException.ThrowIfNull(values);
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

            var offset = 0;

            // Offsets
            _buffer = _writer.EnsureCapacity(_blockIndex, offset, offset + _rows * 8);
            for (var i = 0; i < _rows; i++)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(_buffer.AsSpan(offset, 8), (ulong)offsets[i]);
                offset += 8;
            }

            // Tuple is serialized element-wise: first all Item1 strings, then all Item2 strings.
            for (var i = 0; i < _rows; i++)
            {
                var arr = _collected[i];
                for (var j = 0; j < arr.Length; j++)
                {
                    WriteString(ref offset, arr[j].Item1);
                }
            }

            for (var i = 0; i < _rows; i++)
            {
                var arr = _collected[i];
                for (var j = 0; j < arr.Length; j++)
                {
                    WriteString(ref offset, arr[j].Item2);
                }
            }

            _encoded = true;
            _writer.SetDataLength(_blockIndex, offset);
        }

        private void WriteString(ref int offset, string? value)
        {
            var s = value ?? string.Empty;
            var byteCount = Encoding.UTF8.GetByteCount(s);
            _buffer = _writer.EnsureCapacity(_blockIndex, offset, offset + MaxVarintLen64 + byteCount);
            offset += WriteUtf8StringValue(_buffer.AsSpan(offset), s);
        }
    }
}

internal static class ArrayTupleStringStringTypeName
{
    private const string Prefix = "Array(Tuple(";
    private const string Suffix = "))";

    internal static string Build(string? firstElementName, string? secondElementName)
    {
        var hasFirst = !string.IsNullOrWhiteSpace(firstElementName);
        var hasSecond = !string.IsNullOrWhiteSpace(secondElementName);

        if (hasFirst != hasSecond)
        {
            throw new ArgumentException("Either both tuple element names must be provided or none.");
        }

        return hasFirst
            ? $"{Prefix}{firstElementName} String, {secondElementName} String{Suffix}"
            : $"{Prefix}String, String{Suffix}";
    }

    internal static bool Matches(ReadOnlySpan<byte> typeNameUtf8)
    {
        var typeName = Encoding.UTF8.GetString(typeNameUtf8).Trim();
        if (!typeName.StartsWith(Prefix, StringComparison.Ordinal) || !typeName.EndsWith(Suffix, StringComparison.Ordinal))
        {
            return false;
        }

        var inner = typeName.Substring(Prefix.Length, typeName.Length - Prefix.Length - Suffix.Length);
        var parts = inner.Split(',');
        if (parts.Length != 2)
        {
            return false;
        }

        foreach (var part in parts)
        {
            var tokens = part.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            var elementType = tokens.Length switch
            {
                1 => tokens[0],
                2 => tokens[1],
                _ => null,
            };

            if (!string.Equals(elementType, "String", StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }
}

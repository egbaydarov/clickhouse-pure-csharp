using System.IO;
using System.Text;
using Scriban;

namespace Clickhouse.Pure.ColumnCodeGenerator;

internal static class Program
{
    private static readonly UTF8Encoding Utf8NoBom = new(encoderShouldEmitUTF8Identifier: false);

    private const string TemplatesDir = "templates";
    private const string OutDir = "../../../../Clickhouse.Pure.Columns/";

    private static int Main()
    {
        try
        {
            var numerics = new[]
            {
                // IPAddresses
                new NumericColumnVariant()
                {
                    ClickhouseType = "Date32",
                    CsharpType = "DateOnly",
                    SpanInterpretFunction = "DateOnlyExt.From1970_01_01DaysInt32",
                    ValueSizeInBytes = 4,
                    WriterValueStatements = new[]
                    {
                        "var days = value.DayNumber - 719162;",
                        "BinaryPrimitives.WriteInt32LittleEndian(dest, days);"
                    }
                },
                // IPAddresses
                new NumericColumnVariant
                {
                    ClickhouseType = "Date",
                    CsharpType = "DateOnly",
                    SpanInterpretFunction = "DateOnlyExt.From1970_01_01Days",
                    ValueSizeInBytes = 2,
                    WriterValueStatements = new[]
                    {
                        "var days = value.DayNumber - 719162;",
                        "if ((uint)days > ushort.MaxValue)",
                        "{",
                        "    throw new ArgumentOutOfRangeException(nameof(value), \"DateOnly is outside of ClickHouse Date range (1970-01-01..2106-02-07).\");",
                        "}",
                        "BinaryPrimitives.WriteUInt16LittleEndian(dest, (ushort)days);"
                    }
                },
                new NumericColumnVariant
                {
                    ClickhouseType = "IPv4",
                    CsharpType = "IPAddress",
                    SpanInterpretFunction = "IpAddressExt.FromLittleEndianIPv4",
                    ValueSizeInBytes = 4,
                    WriterValueStatements = new[]
                    {
                        "IpAddressExt.WriteLittleEndianIPv4(dest, value);"
                    }
                },

                // Bool (same as UInt8)
                new NumericColumnVariant
                {
                    ClickhouseType = "Bool",
                    CsharpType = "bool",
                    SpanInterpretFunction = "MemoryMarshal.Read<bool>",
                    ValueSizeInBytes = 1,
                    WriterValueStatements = new[]
                    {
                        "dest[0] = value ? (byte)1 : (byte)0;"
                    }
                },

                // Unsigned ints
                new NumericColumnVariant
                {
                    ClickhouseType = "UInt8",
                    CsharpType = "byte",
                    SpanInterpretFunction = "MemoryMarshal.Read<byte>",
                    ValueSizeInBytes = 1,
                    WriterValueStatements = new[]
                    {
                        "dest[0] = value;"
                    }
                },
                new NumericColumnVariant
                {
                    ClickhouseType = "UInt16",
                    CsharpType = "ushort",
                    SpanInterpretFunction = "BinaryPrimitives.ReadUInt16LittleEndian",
                    ValueSizeInBytes = 2,
                    WriterValueStatements = new[]
                    {
                        "BinaryPrimitives.WriteUInt16LittleEndian(dest, value);"
                    }
                },
                new NumericColumnVariant
                {
                    ClickhouseType = "UInt32",
                    CsharpType = "uint",
                    SpanInterpretFunction = "BinaryPrimitives.ReadUInt32LittleEndian",
                    ValueSizeInBytes = 4,
                    WriterValueStatements = new[]
                    {
                        "BinaryPrimitives.WriteUInt32LittleEndian(dest, value);"
                    }
                },
                new NumericColumnVariant
                {
                    ClickhouseType = "UInt64",
                    CsharpType = "ulong",
                    SpanInterpretFunction = "BinaryPrimitives.ReadUInt64LittleEndian",
                    ValueSizeInBytes = 8,
                    WriterValueStatements = new[]
                    {
                        "BinaryPrimitives.WriteUInt64LittleEndian(dest, value);"
                    }
                },
                new NumericColumnVariant
                {
                    ClickhouseType = "UInt128",
                    CsharpType = "UInt128",
                    SpanInterpretFunction = "BinaryPrimitives.ReadUInt128LittleEndian",
                    ValueSizeInBytes = 16,
                    WriterValueStatements = new[]
                    {
                        "BinaryPrimitives.WriteUInt128LittleEndian(dest, value);"
                    }
                },

                // Signed ints
                new NumericColumnVariant
                {
                    ClickhouseType = "Int8",
                    CsharpType = "sbyte",
                    SpanInterpretFunction = "MemoryMarshal.Read<sbyte>",
                    ValueSizeInBytes = 1,
                    WriterValueStatements = new[]
                    {
                        "dest[0] = unchecked((byte)value);"
                    }
                },
                new NumericColumnVariant
                {
                    ClickhouseType = "Int16",
                    CsharpType = "short",
                    SpanInterpretFunction = "BinaryPrimitives.ReadInt16LittleEndian",
                    ValueSizeInBytes = 2,
                    WriterValueStatements = new[]
                    {
                        "BinaryPrimitives.WriteInt16LittleEndian(dest, value);"
                    }
                },
                new NumericColumnVariant
                {
                    ClickhouseType = "Int32",
                    CsharpType = "int",
                    SpanInterpretFunction = "BinaryPrimitives.ReadInt32LittleEndian",
                    ValueSizeInBytes = 4,
                    WriterValueStatements = new[]
                    {
                        "BinaryPrimitives.WriteInt32LittleEndian(dest, value);"
                    }
                },
                new NumericColumnVariant
                {
                    ClickhouseType = "Int64",
                    CsharpType = "long",
                    SpanInterpretFunction = "BinaryPrimitives.ReadInt64LittleEndian",
                    ValueSizeInBytes = 8,
                    WriterValueStatements = new[]
                    {
                        "BinaryPrimitives.WriteInt64LittleEndian(dest, value);"
                    }
                },
                new NumericColumnVariant
                {
                    ClickhouseType = "Int128",
                    CsharpType = "Int128",
                    SpanInterpretFunction = "BinaryPrimitives.ReadInt128LittleEndian",
                    ValueSizeInBytes = 16,
                    WriterValueStatements = new[]
                    {
                        "BinaryPrimitives.WriteInt128LittleEndian(dest, value);"
                    }
                },

                // Floating
                new NumericColumnVariant
                {
                    ClickhouseType = "Float32",
                    CsharpType = "float",
                    SpanInterpretFunction = "BinaryPrimitives.ReadSingleLittleEndian",
                    ValueSizeInBytes = 4,
                    WriterValueStatements = new[]
                    {
                        "BinaryPrimitives.WriteSingleLittleEndian(dest, value);"
                    }
                },
                new NumericColumnVariant
                {
                    ClickhouseType = "Float64",
                    CsharpType = "double",
                    SpanInterpretFunction = "BinaryPrimitives.ReadDoubleLittleEndian",
                    ValueSizeInBytes = 8,
                    WriterValueStatements = new[]
                    {
                        "BinaryPrimitives.WriteDoubleLittleEndian(dest, value);"
                    }
                }
            };

            var nullable = new[]
            {
                new NullableColumnVariant
                {
                    ClickhouseType = "Nullable(String)",
                    CsharpType     = "string?",
                }
            };

            var lowCardinality = new[]
            {
                new LowCardinalityColumnVariant
                {
                    ClickhouseType = "LowCardinality(String)",
                    CsharpType     = "string",
                }
            };

            var fixedString = new[]
            {
                new FixedStringColumnVariant()
                {
                    // 0 means any (will be parsed in runtime)
                    Size = 0,
                }
            };

            Directory.CreateDirectory(OutDir);

            var jobs = new[]
            {
                new TemplateJob(
                    TemplatePath: $"{TemplatesDir}/NumericColumn.scriban-cs",
                    Model: new { NumericTypes = numerics },
                    OutputPath: $"{OutDir}NativeFormat.NumericColumns.generated.cs",
                    SuccessMessage: "Wrote NativeFormat.NumericColumns.generated.cs"),

                new TemplateJob(
                    TemplatePath: $"{TemplatesDir}/NullableColumn.scriban-cs",
                    Model: new { NullableTypes = nullable },
                    OutputPath: $"{OutDir}NativeFormat.NullableColumns.generated.cs",
                    SuccessMessage: "Wrote NativeFormat.NullableColumns.generated.cs"),

                new TemplateJob(
                    TemplatePath: $"{TemplatesDir}/LowCardinalityColumn.scriban-cs",
                    Model: new { LowCardinalityTypes = lowCardinality },
                    OutputPath: $"{OutDir}NativeFormat.LowCardinalityColumn.generated.cs",
                    SuccessMessage: "Wrote NativeFormat.LowCardinalityColumn.generated.cs"),

                new TemplateJob(
                    TemplatePath: $"{TemplatesDir}/FixedStringColumn.scriban-cs",
                    Model: new { FixedStringTypes = fixedString },
                    OutputPath: $"{OutDir}NativeFormat.FixedStringColumn.generated.cs",
                    SuccessMessage: "Wrote NativeFormat.FixedStringColumn.generated.cs"),
            };

            foreach (var job in jobs)
            {
                var content = RenderTemplate(job.TemplatePath, job.Model);
                File.WriteAllText(job.OutputPath, content, Utf8NoBom);
                Console.WriteLine(job.SuccessMessage);
            }

            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.ToString());
            return 2;
        }
    }

    private static string RenderTemplate(string templatePath, object model)
    {
        var tplText = File.ReadAllText(templatePath, Encoding.UTF8);
        var tpl = Template.Parse(tplText);

        if (tpl.HasErrors)
        {
            var sb = new StringBuilder()
                .AppendLine($"Template parse errors in '{templatePath}':");
            foreach (var m in tpl.Messages)
                sb.AppendLine(" - " + m);
            throw new InvalidOperationException(sb.ToString());
        }

        return tpl.Render(model, memberRenamer: mi => mi.Name);
    }

    private sealed record TemplateJob(
        string TemplatePath,
        object Model,
        string OutputPath,
        string SuccessMessage);
}

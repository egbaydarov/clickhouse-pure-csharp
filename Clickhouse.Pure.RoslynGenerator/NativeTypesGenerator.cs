using System;
using System.IO;
using Microsoft.CodeAnalysis;

namespace Clickhouse.Pure.RoslynGenerator;

[Generator]
public class NativeTypesGenerator : ISourceGenerator
{
    private static readonly DiagnosticDescriptor InformationalMessageDescriptor = new(
        id: "OCGINFO01",
        title: "Informational Message",
        messageFormat: "{0}",
        category: "SourceGenerator",
        defaultSeverity: DiagnosticSeverity.Info,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor ErrorDescriptor = new(
        id: "OCGERRO01",
        title: "Source Generator Error",
        messageFormat: "{0}",
        category: "SourceGenerator",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public void Initialize(GeneratorInitializationContext context)
    {
    }

    public void Execute(GeneratorExecutionContext context)
    {
        try
        {
            var outputPath = "TODO";
            AddGeneratedFilesToCompilation(context, outputPath);

            Directory.Delete(outputPath, true);
        }
        catch (Exception ex)
        {
            ReportError(context, ex.ToString());
        }
    }

    private static void AddGeneratedFilesToCompilation(GeneratorExecutionContext context, string outputPath)
    {
        foreach (var file in Directory.EnumerateFiles(outputPath, "*.cs", SearchOption.AllDirectories))
        {
            var sourceText = File.ReadAllText(file);
            context.AddSource(Path.GetFileNameWithoutExtension(file) + ".g.cs", sourceText);
        }
    }

    private static void Log(GeneratorExecutionContext context, string message)
    {
        context.ReportDiagnostic(Diagnostic.Create(
            descriptor: InformationalMessageDescriptor,
            location: Location.None,
            messageArgs: message));
    }

    private static void ReportError(GeneratorExecutionContext context, string message)
    {
        context.ReportDiagnostic(Diagnostic.Create(
            descriptor: ErrorDescriptor,
            location: Location.None,
            messageArgs: message));
    }
}

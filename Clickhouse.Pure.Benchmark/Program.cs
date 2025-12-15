using BenchmarkDotNet.Running;

namespace Clickhouse.Pure.BenchmarkDotnet;

public static class Program
{
    public static void Main(string[] args)
    {
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}

```

BenchmarkDotNet v0.15.6, Linux Ubuntu 24.04.3 LTS (Noble Numbat)
Intel Xeon Platinum 8370C CPU 2.80GHz (Max: 3.49GHz), 1 CPU, 2 logical cores and 1 physical core
.NET SDK 9.0.307
  [Host]    : .NET 9.0.11 (9.0.11, 9.0.1125.51716), X64 RyuJIT x86-64-v4
  MediumRun : .NET 9.0.11 (9.0.11, 9.0.1125.51716), X64 RyuJIT x86-64-v4

Job=MediumRun  InvocationCount=1  IterationCount=15  
LaunchCount=2  UnrollFactor=1  WarmupCount=10  

```
| Method                                  | RowCount | Mean          | Error       | StdDev        | Median        | P90           | Gen0        | Gen1       | Allocated     |
|---------------------------------------- |--------- |--------------:|------------:|--------------:|--------------:|--------------:|------------:|-----------:|--------------:|
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000**    |      **6.816 ms** |   **0.5761 ms** |     **0.8262 ms** |      **6.598 ms** |      **8.055 ms** |           **-** |          **-** |     **665.41 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000    |     22.546 ms |   2.8388 ms |     3.7897 ms |     21.665 ms |     23.120 ms |           - |          - |    3542.45 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **100000**   |     **58.177 ms** |  **12.3430 ms** |    **18.4744 ms** |     **48.746 ms** |     **83.329 ms** |           **-** |          **-** |   **14709.29 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 100000   |    184.721 ms |  23.9513 ms |    35.8491 ms |    167.759 ms |    249.152 ms |   1000.0000 |          - |    28153.8 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **1000000**  |    **406.484 ms** |  **39.9876 ms** |    **57.3490 ms** |    **395.359 ms** |    **483.088 ms** |           **-** |          **-** |   **65034.41 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 1000000  |  1,654.493 ms | 113.5265 ms |   166.4056 ms |  1,604.829 ms |  1,888.508 ms |  11000.0000 |  7000.0000 |  274343.84 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000000** | **11,595.943 ms** | **939.4280 ms** | **1,377.0014 ms** | **11,383.749 ms** | **13,134.824 ms** |           **-** |          **-** | **2157607.41 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000000 | 19,105.675 ms | 469.6018 ms |   702.8779 ms | 19,018.711 ms | 19,974.067 ms | 111000.0000 | 64000.0000 | 3127059.77 KB |

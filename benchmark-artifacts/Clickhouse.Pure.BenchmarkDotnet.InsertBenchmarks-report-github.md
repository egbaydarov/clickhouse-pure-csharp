```

BenchmarkDotNet v0.15.6, Linux Ubuntu 24.04.3 LTS (Noble Numbat)
AMD EPYC 7763 3.24GHz, 1 CPU, 2 logical cores and 1 physical core
.NET SDK 9.0.306
  [Host]    : .NET 9.0.10 (9.0.10, 9.0.1025.47515), X64 RyuJIT x86-64-v3
  MediumRun : .NET 9.0.10 (9.0.10, 9.0.1025.47515), X64 RyuJIT x86-64-v3

Job=MediumRun  InvocationCount=1  IterationCount=15  
LaunchCount=2  UnrollFactor=1  WarmupCount=10  

```
| Method                                  | RowCount | Mean         | Error     | StdDev     | Median       | P90          | Gen0       | Gen1       | Allocated    |
|---------------------------------------- |--------- |-------------:|----------:|-----------:|-------------:|-------------:|-----------:|-----------:|-------------:|
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000**    |     **7.384 ms** |  **1.284 ms** |   **1.799 ms** |     **6.744 ms** |     **9.648 ms** |          **-** |          **-** |    **665.41 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000    |    23.337 ms |  1.323 ms |   1.811 ms |    22.458 ms |    25.602 ms |          - |          - |   3796.88 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **100000**   |    **69.948 ms** | **12.102 ms** |  **18.114 ms** |    **59.659 ms** |    **97.039 ms** |          **-** |          **-** |   **6517.27 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 100000   |   193.073 ms | 37.553 ms |  55.044 ms |   169.557 ms |   254.921 ms |  1000.0000 |          - |  30199.86 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **1000000**  |   **420.804 ms** | **32.040 ms** |  **42.772 ms** |   **419.810 ms** |   **467.572 ms** |          **-** |          **-** |  **65037.19 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 1000000  | 1,590.216 ms | 79.443 ms | 111.368 ms | 1,578.308 ms | 1,722.423 ms | 16000.0000 | 10000.0000 | 282534.64 KB |

```

BenchmarkDotNet v0.15.6, Linux Ubuntu 24.04.3 LTS (Noble Numbat)
AMD EPYC 7763 3.24GHz, 1 CPU, 2 logical cores and 1 physical core
.NET SDK 9.0.306
  [Host]    : .NET 9.0.10 (9.0.10, 9.0.1025.47515), X64 RyuJIT x86-64-v3
  MediumRun : .NET 9.0.10 (9.0.10, 9.0.1025.47515), X64 RyuJIT x86-64-v3

Job=MediumRun  InvocationCount=1  IterationCount=15  
LaunchCount=2  UnrollFactor=1  WarmupCount=10  

```
| Method                                  | RowCount | Column               | Mean       | Error      | StdDev     | Median     | P90        | Gen0      | Gen1      | Allocated    |
|---------------------------------------- |--------- |--------------------- |-----------:|-----------:|-----------:|-----------:|-----------:|----------:|----------:|-------------:|
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000**    | **DateTime64(6)**        |   **2.873 ms** |  **0.1154 ms** |  **0.1618 ms** |   **2.826 ms** |   **3.058 ms** |         **-** |         **-** |     **92.44 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000    | DateTime64(6)        |   7.293 ms |  0.9036 ms |  1.2063 ms |   6.953 ms |   7.558 ms |         - |         - |   1553.23 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000**    | **FixedString(16)**      |   **3.755 ms** |  **0.6211 ms** |  **0.8908 ms** |   **3.393 ms** |   **4.988 ms** |         **-** |         **-** |    **171.32 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000    | FixedString(16)      |   6.573 ms |  0.1912 ms |  0.2681 ms |   6.547 ms |   6.998 ms |         - |         - |   1632.19 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000**    | **Int128**               |   **4.221 ms** |  **0.9530 ms** |  **1.3969 ms** |   **3.427 ms** |   **6.052 ms** |         **-** |         **-** |    **171.23 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000    | Int128               |  14.143 ms |  4.8707 ms |  6.8281 ms |   9.894 ms |  22.712 ms |         - |         - |   2334.06 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000**    | **Int64**                |   **2.728 ms** |  **0.1185 ms** |  **0.1541 ms** |   **2.722 ms** |   **2.882 ms** |         **-** |         **-** |     **92.37 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000    | Int64                |   8.061 ms |  1.9409 ms |  2.7209 ms |   6.822 ms |  11.575 ms |         - |         - |   1474.37 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000**    | **LowCa(...)ring) [22]** |   **2.717 ms** |  **0.0801 ms** |  **0.1148 ms** |   **2.706 ms** |   **2.884 ms** |         **-** |         **-** |     **63.24 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000    | LowCa(...)ring) [22] |   6.998 ms |  0.1879 ms |  0.2695 ms |   6.940 ms |   7.282 ms |         - |         - |   1241.15 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000**    | **String**               |   **6.793 ms** |  **2.2137 ms** |  **3.1033 ms** |   **6.075 ms** |  **10.971 ms** |         **-** |         **-** |    **141.64 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000    | String               |   8.117 ms |  2.5364 ms |  3.6376 ms |   6.674 ms |  13.129 ms |         - |         - |   1368.98 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **100000**   | **DateTime64(6)**        |  **10.708 ms** |  **1.8811 ms** |  **2.8156 ms** |   **9.683 ms** |  **14.173 ms** |         **-** |         **-** |    **802.09 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 100000   | DateTime64(6)        |  53.707 ms | 11.5632 ms | 16.9492 ms |  49.846 ms |  75.455 ms |         - |         - |    8074.8 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **100000**   | **FixedString(16)**      |  **28.747 ms** | **15.1515 ms** | **21.7299 ms** |  **17.729 ms** |  **61.593 ms** |         **-** |         **-** |   **1590.64 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 100000   | FixedString(16)      |  65.455 ms | 16.0327 ms | 23.5005 ms |  52.725 ms | 100.522 ms |         - |         - |   9881.13 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **100000**   | **Int128**               |  **13.118 ms** |  **3.4084 ms** |  **4.9960 ms** |  **10.942 ms** |  **20.916 ms** |         **-** |         **-** |   **1591.04 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 100000   | Int128               |  87.464 ms | 24.4049 ms | 35.0008 ms |  79.224 ms | 140.160 ms |         - |         - |  16911.17 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **100000**   | **Int64**                |   **8.226 ms** |  **1.6296 ms** |  **2.4391 ms** |   **7.100 ms** |  **11.221 ms** |         **-** |         **-** |    **802.02 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 100000   | Int64                |  71.043 ms | 16.8592 ms | 24.7119 ms |  66.749 ms |  97.551 ms |         - |         - |   8316.88 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **100000**   | **LowCa(...)ring) [22]** |   **7.691 ms** |  **1.2449 ms** |  **1.7041 ms** |   **8.315 ms** |   **8.853 ms** |         **-** |         **-** |    **503.59 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 100000   | LowCa(...)ring) [22] |  84.038 ms | 17.1717 ms | 25.1700 ms |  76.390 ms | 114.847 ms |         - |         - |   4949.63 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **100000**   | **String**               |  **23.690 ms** |  **5.8665 ms** |  **8.5991 ms** |  **21.216 ms** |  **33.087 ms** |         **-** |         **-** |    **1295.4 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 100000   | String               |  44.485 ms | 11.4025 ms | 16.7137 ms |  40.581 ms |  69.522 ms |         - |         - |   5973.63 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **1000000**  | **DateTime64(6)**        |  **76.462 ms** | **11.3249 ms** | **16.2418 ms** |  **75.509 ms** |  **94.722 ms** |         **-** |         **-** |   **7899.59 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 1000000  | DateTime64(6)        | 343.017 ms | 39.3929 ms | 57.7417 ms | 340.796 ms | 407.421 ms | 3000.0000 | 2000.0000 |  79690.09 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **1000000**  | **FixedString(16)**      | **120.183 ms** | **18.4421 ms** | **25.8532 ms** | **114.848 ms** | **153.721 ms** |         **-** |         **-** |  **15785.61 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 1000000  | FixedString(16)      | 368.732 ms | 38.7661 ms | 55.5971 ms | 355.841 ms | 442.676 ms | 4000.0000 | 2000.0000 |  87499.31 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **1000000**  | **Int128**               |  **94.268 ms** | **20.7406 ms** | **30.4013 ms** |  **89.726 ms** | **146.674 ms** |         **-** |         **-** |  **15785.36 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 1000000  | Int128               | 591.823 ms | 48.6896 ms | 68.2561 ms | 590.668 ms | 674.632 ms | 8000.0000 | 4000.0000 | 157830.49 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **1000000**  | **Int64**                |  **58.480 ms** | **13.3759 ms** | **19.1833 ms** |  **55.286 ms** |  **83.437 ms** |         **-** |         **-** |  **16090.76 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 1000000  | Int64                | 376.250 ms | 44.5617 ms | 65.3180 ms | 352.857 ms | 458.364 ms | 3000.0000 | 2000.0000 |  63686.41 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **1000000**  | **LowCa(...)ring) [22]** |  **45.896 ms** |  **9.7264 ms** | **14.5580 ms** |  **40.620 ms** |  **69.987 ms** |         **-** |         **-** |   **4906.29 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 1000000  | LowCa(...)ring) [22] | 334.107 ms | 42.1407 ms | 59.0754 ms | 312.328 ms | 395.547 ms | 1000.0000 |         - |   40243.5 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **1000000**  | **String**               | **149.734 ms** | **14.1409 ms** | **19.8235 ms** | **145.140 ms** | **176.832 ms** |         **-** |         **-** |  **12828.05 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 1000000  | String               | 265.179 ms | 19.4991 ms | 26.6906 ms | 253.937 ms | 298.313 ms | 1000.0000 |         - |  40244.47 KB |

```

BenchmarkDotNet v0.15.6, Linux Ubuntu 24.04.3 LTS (Noble Numbat)
Intel Xeon Platinum 8370C CPU 2.80GHz (Max: 3.49GHz), 1 CPU, 2 logical cores and 1 physical core
.NET SDK 9.0.307
  [Host]    : .NET 9.0.11 (9.0.11, 9.0.1125.51716), X64 RyuJIT x86-64-v4
  MediumRun : .NET 9.0.11 (9.0.11, 9.0.1125.51716), X64 RyuJIT x86-64-v4

Job=MediumRun  InvocationCount=1  IterationCount=15  
LaunchCount=2  UnrollFactor=1  WarmupCount=10  

```
| Method                                  | RowCount | Column               | Mean       | Error      | StdDev      | Median     | P90        | Gen0      | Gen1      | Allocated    |
|---------------------------------------- |--------- |--------------------- |-----------:|-----------:|------------:|-----------:|-----------:|----------:|----------:|-------------:|
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000**    | **DateTime64(6)**        |   **5.579 ms** |  **0.8591 ms** |   **1.2858 ms** |   **5.533 ms** |   **6.941 ms** |         **-** |         **-** |     **92.44 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000    | DateTime64(6)        |  19.266 ms |  7.8494 ms |  11.5055 ms |  14.888 ms |  37.439 ms |         - |         - |   1426.98 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000**    | **FixedString(16)**      |   **4.173 ms** |  **0.2778 ms** |   **0.3803 ms** |   **4.135 ms** |   **4.796 ms** |         **-** |         **-** |    **171.32 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000    | FixedString(16)      |   8.385 ms |  0.9353 ms |   1.2803 ms |   8.018 ms |   9.269 ms |         - |         - |   1505.55 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000**    | **Int128**               |   **4.864 ms** |  **0.9572 ms** |   **1.4031 ms** |   **4.371 ms** |   **7.119 ms** |         **-** |         **-** |    **171.23 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000    | Int128               |  20.008 ms |  6.0306 ms |   8.6490 ms |  19.951 ms |  31.556 ms |         - |         - |   2208.16 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000**    | **Int64**                |   **3.264 ms** |  **0.6512 ms** |   **0.9545 ms** |   **2.956 ms** |   **5.222 ms** |         **-** |         **-** |     **92.76 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000    | Int64                |   9.517 ms |  1.6247 ms |   2.3814 ms |   8.407 ms |  13.091 ms |         - |         - |   1349.36 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000**    | **LowCa(...)ring) [22]** |   **6.146 ms** |  **2.5309 ms** |   **3.7882 ms** |   **4.436 ms** |  **12.503 ms** |         **-** |         **-** |     **63.24 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000    | LowCa(...)ring) [22] |   9.168 ms |  1.4080 ms |   1.9738 ms |   8.597 ms |  12.326 ms |         - |         - |   1114.85 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **10000**    | **String**               |   **6.422 ms** |  **1.1354 ms** |   **1.6283 ms** |   **6.864 ms** |   **8.230 ms** |         **-** |         **-** |    **143.23 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 10000    | String               |   8.667 ms |  1.6472 ms |   2.3624 ms |   7.415 ms |  12.606 ms |         - |         - |   1114.56 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **100000**   | **DateTime64(6)**        |  **14.293 ms** |  **4.3376 ms** |   **6.4923 ms** |  **12.786 ms** |  **24.722 ms** |         **-** |         **-** |    **802.09 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 100000   | DateTime64(6)        |  55.483 ms | 10.6839 ms |  15.9911 ms |  51.680 ms |  80.982 ms |         - |         - |   7052.41 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **100000**   | **FixedString(16)**      |  **19.346 ms** |  **4.0813 ms** |   **5.7213 ms** |  **17.139 ms** |  **28.313 ms** |         **-** |         **-** |   **1590.64 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 100000   | FixedString(16)      |  44.556 ms | 13.6852 ms |  19.1847 ms |  39.139 ms |  65.249 ms |         - |         - |   7833.82 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **100000**   | **Int128**               |  **11.247 ms** |  **1.8738 ms** |   **2.7466 ms** |  **11.203 ms** |  **14.783 ms** |         **-** |         **-** |   **1590.88 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 100000   | Int128               | 111.508 ms | 26.8878 ms |  39.4118 ms | 105.995 ms | 159.239 ms |         - |         - |  14865.21 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **100000**   | **Int64**                |  **10.876 ms** |  **4.1970 ms** |   **6.1519 ms** |   **9.293 ms** |  **21.496 ms** |         **-** |         **-** |    **802.02 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 100000   | Int64                |  60.899 ms | 10.7930 ms |  15.8202 ms |  58.238 ms |  80.436 ms |         - |         - |   6270.99 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **100000**   | **LowCa(...)ring) [22]** |  **11.287 ms** |  **5.4778 ms** |   **7.4981 ms** |   **7.771 ms** |  **22.584 ms** |         **-** |         **-** |    **503.59 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 100000   | LowCa(...)ring) [22] |  76.284 ms | 23.5228 ms |  34.4794 ms |  59.368 ms | 113.458 ms |         - |         - |   3927.77 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **100000**   | **String**               |  **24.920 ms** |  **5.1403 ms** |   **7.5345 ms** |  **24.092 ms** |  **32.365 ms** |         **-** |         **-** |   **1294.91 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 100000   | String               |  50.004 ms |  7.5390 ms |  10.3194 ms |  47.215 ms |  65.213 ms |         - |         - |   3926.97 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **1000000**  | **DateTime64(6)**        |  **82.061 ms** | **16.1243 ms** |  **23.1249 ms** |  **77.357 ms** | **112.301 ms** |         **-** |         **-** |    **7900.6 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 1000000  | DateTime64(6)        | 372.550 ms | 50.0804 ms |  73.4072 ms | 360.730 ms | 460.267 ms | 2000.0000 | 1000.0000 |  63307.62 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **1000000**  | **FixedString(16)**      | **128.475 ms** | **20.4386 ms** |  **29.3124 ms** | **115.741 ms** | **163.321 ms** |         **-** |         **-** |  **15785.23 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 1000000  | FixedString(16)      | 365.985 ms | 55.4405 ms |  82.9808 ms | 318.489 ms | 458.614 ms | 2000.0000 | 1000.0000 |  71117.72 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **1000000**  | **Int128**               |  **92.305 ms** | **19.2768 ms** |  **27.6462 ms** |  **91.595 ms** | **117.416 ms** |         **-** |         **-** |  **15784.45 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 1000000  | Int128               | 638.642 ms | 58.8469 ms |  82.4951 ms | 614.763 ms | 746.562 ms | 5000.0000 | 3000.0000 | 141447.23 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **1000000**  | **Int64**                |  **55.885 ms** | **12.3694 ms** |  **17.7399 ms** |  **50.802 ms** |  **79.476 ms** |         **-** |         **-** |    **7899.6 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 1000000  | Int64                | 396.654 ms | 67.8097 ms | 101.4944 ms | 360.947 ms | 532.927 ms | 2000.0000 | 1000.0000 |  55495.96 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **1000000**  | **LowCa(...)ring) [22]** |  **55.442 ms** | **15.8815 ms** |  **23.7707 ms** |  **42.198 ms** |  **89.741 ms** |         **-** |         **-** |   **4906.29 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 1000000  | LowCa(...)ring) [22] | 390.507 ms | 61.9128 ms |  88.7934 ms | 347.992 ms | 514.837 ms | 1000.0000 |         - |   32055.8 KB |
| **&#39;Pure gRPC driver (Native format)&#39;**      | **1000000**  | **String**               | **141.040 ms** | **20.6143 ms** |  **30.2161 ms** | **131.782 ms** | **180.077 ms** |         **-** |         **-** |  **12828.25 KB** |
| &#39;Official ClickHouse.Client (BulkCopy)&#39; | 1000000  | String               | 276.790 ms | 33.0340 ms |  47.3764 ms | 256.244 ms | 338.904 ms | 1000.0000 |         - |  32053.88 KB |

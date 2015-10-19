# parallel-mergesort
parallel implementation mergesort algorithm with large files support.

Input file splits on equal chunks, they are sorts separately, and then pairwise merges into output or intermediate result for additional merge steps.
Sort/merge steps performs concurrently. Intermediate results can be written in temporary files or stay in memory if available.

Benchmarking results(with 96Mb chunks, on 8core cpu)
|               |     10kB      |      1MB      |     100MB     |      1GB      |      10GB     |
| ------------- | ------------- | ------------- | ------------- | ------------- | ------------- |
|   1 thread    |   0m0.103s    |   0m0.176s    |   0m3.783s    |   0m53.993s   |   15m47.377s  |
|   2 thread    |   0m0.093s    |   0m0.178s    |   0m2.662s    |   0m39.557s   |   11m25.370s  |
|   4 thread    |   0m0.094s    |   0m0.182s    |   0m2.493s    |   0m35.678s   |    11m9.066s  |
|   8 thread    |   0m0.096s    |   0m0.174s    |   0m2.621s    |   0m37.974s   |   11m43.442s  |
|  32 thread    |   0m0.105s    |   0m0.151s    |   0m5.231s    |    1m4.600s   |   14m20.195s  |

Up to 100Mb computation performed in memory. For 1GB ang 10GB file temporary disk storage used. 


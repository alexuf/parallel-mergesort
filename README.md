# parallel-mergesort
parallel implementation mergesort algorithm with large files support.

Input file splits on equal chunks, they are sorts separately, and then pairwise merges into output or intermediate result for additional merge steps.
Sort/merge steps performs concurrently.  Intermediate results can be written in temporary files or stay in memory if available.

|               |     10kB      |      1MB      |     100MB     |      10GB     |
| ------------- | ------------- | ------------- | ------------- | ------------- |
|   1 thread    |   0m0.074s    |   0m0.160s    |   0m3.767s    |   66m28.210s  |
|   2 thread    |   0m0.070s    |   0m0.152s    |   0m2.700s    |   15m38.408s  |
|   4 thread    |   0m0.075s    |   0m0.153s    |   0m2.294s    |   15m16.214s  |
|   8 thread    |   0m0.074s    |   0m0.153s    |   0m2.365s    |   12m29.247s  |
|  32 thread    |   0m0.071s    |   0m0.151s    |   0m2.941s    |   15m10.694s  |

Up to 100Mb computation performed in memory. For 10GB file temporary disk storage used. 
Merges performs concurrently as soon sort or previous merge results available, and in 1 thread case there is filesystem overhead when large files merges with small sort results.
With 8 threads on 8 core cpu there are best results.

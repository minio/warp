# Analyzing `warp` results

When benchmarks have finished all request data will be saved to a file and an analysis will be shown. The saved data can be re-evaluated by running `warp analyze (filename)`.

## Analysis Data

All analysis will be done on a reduced part of the full data.  The data aggregation will *start* when all threads have completed one request
 and the time segment will *stop* when the last request of a thread is initiated.

This is to exclude variations due to warm-up and threads finishing at different times.
Therefore the analysis time will typically be slightly below the selected benchmark duration.

In this run "only" 42.9 seconds are included in the aggregate data, due to big payload size and low throughput:
```
Operation: PUT
* Average: 37.19 MiB/s, 0.37 obj/s, 0.33 ops ended/s (42.957s)
```

The benchmark run is then divided into fixed duration *segments* specified by `-analyze.dur`, default 1s. 
For each segment the throughput is calculated across all threads.

The analysis output will display the fastest, slowest and 50% median segment.
```
Aggregated, split into 59 x 1s time segments:
* Fastest: 2693.83 MiB/s, 269.38 obj/s, 269.00 ops ended/s (1s)
* 50% Median: 2419.56 MiB/s, 241.96 obj/s, 240.00 ops ended/s (1s)
* Slowest: 1137.36 MiB/s, 113.74 obj/s, 112.00 ops ended/s (1s)
```

### Analysis Parameters

Beside the important `--analysis.dur` which specifies the time segment size for 
aggregated data there are some additional parameters that can be used.

Specifying `--analyze.hostdetails` will output time aggregated data per host instead of just averages. 
For instance:

```
Throughput by host:
 * http://127.0.0.1:9001: Avg: 81.48 MiB/s, 81.48 obj/s (4m59.976s)
        - Fastest: 86.46 MiB/s, 86.46 obj/s (1s)
        - 50% Median: 82.23 MiB/s, 82.23 obj/s (1s)
        - Slowest: 68.14 MiB/s, 68.14 obj/s (1s)
 * http://127.0.0.1:9002: Avg: 81.48 MiB/s, 81.48 obj/s (4m59.968s)
        - Fastest: 87.36 MiB/s, 87.36 obj/s (1s)
        - 50% Median: 82.28 MiB/s, 82.28 obj/s (1s)
        - Slowest: 68.40 MiB/s, 68.40 obj/s (1s)
```


`--analyze.op=GET` will only analyze GET operations.

Specifying `--analyze.host=http://127.0.0.1:9001` will only consider data from this specific host.

Warp will automatically discard the time taking the first and last request of all threads to finish.
However, if you would like to discard additional time from the aggregated data,
this is possible. For instance `analyze.skip=10s` will skip the first 10 seconds of data for each operation type.

Note that skipping data will not always result in the exact reduction in time for the aggregated data
since the start time will still be aligned with requests starting.

### Per Request Statistics

By adding the `--requests` parameter it is possible to display per request statistics.

This is not enabled by default, since it is assumed the benchmarks are throughput limited,
but in certain scenarios it can be useful to determine problems with individual hosts for instance.

Example:

```
Operation: GET. Concurrency: 12. Hosts: 7.

Requests - 16720:
 * Fastest: 2.9965ms Slowest: 62.9993ms 50%: 21.0006ms 90%: 31.0021ms 99%: 41.0016ms
 * First Byte: Average: 20.575134ms, Median: 20.0007ms, Best: 1.9985ms, Worst: 62.9993ms

Requests by host:
 * http://127.0.0.1:9001 - 2395 requests:
        - Fastest: 2.9965ms Slowest: 55.0015ms 50%: 18.0002ms 90%: 28.001ms
        - First Byte: Average: 17.139147ms, Median: 16.9998ms, Best: 1.9985ms, Worst: 53.0026ms
 * http://127.0.0.1:9002 - 2395 requests:
        - Fastest: 4.999ms Slowest: 60.9925ms 50%: 20.9993ms 90%: 31.001ms
        - First Byte: Average: 20.174683ms, Median: 19.9996ms, Best: 3.999ms, Worst: 59.9912ms
 * http://127.0.0.1:9003 - 2395 requests:
        - Fastest: 6.9988ms Slowest: 56.0005ms 50%: 20.9978ms 90%: 31.001ms
        - First Byte: Average: 20.272876ms, Median: 19.9983ms, Best: 5.0012ms, Worst: 55.0012ms
 * http://127.0.0.1:9004 - 2395 requests:
        - Fastest: 5.0002ms Slowest: 62.9993ms 50%: 22.0009ms 90%: 33.001ms
        - First Byte: Average: 22.039164ms, Median: 21.0015ms, Best: 4.0003ms, Worst: 62.9993ms
 * http://127.0.0.1:9005 - 2396 requests:
        - Fastest: 6.9934ms Slowest: 54.002ms 50%: 21.0008ms 90%: 30.9998ms
        - First Byte: Average: 20.871833ms, Median: 20.0006ms, Best: 4.9998ms, Worst: 52.0019ms
 * http://127.0.0.1:9006 - 2396 requests:
        - Fastest: 6.0019ms Slowest: 54.9972ms 50%: 22.9985ms 90%: 33.0007ms
        - First Byte: Average: 22.430863ms, Median: 21.9986ms, Best: 5.0008ms, Worst: 53.9981ms
 * http://127.0.0.1:9007 - 2396 requests:
        - Fastest: 7.9968ms Slowest: 55.0899ms 50%: 21.998ms 90%: 30.9998ms
        - First Byte: Average: 21.049681ms, Median: 20.9989ms, Best: 6.9958ms, Worst: 54.0884ms
```

The fastest and slowest request times are shown, as well as selected 
percentiles and the total amount is requests considered.

Note that different metrics are used to select the number of requests per host and for the combined, 
so there will likely be differences.

### Time Series CSV Output

It is possible to output the CSV data of analysis using `--analyze.out=filename.csv` 
which will write the CSV data to the specified file.

These are the data fields exported:

| Header              | Description                                                                                       |
|---------------------|---------------------------------------------------------------------------------------------------|
| `index`             | Index of the segment                                                                              |
| `op`                | Operation executed                                                                                |
| `host`              | If only one host, host name, otherwise empty                                                      |
| `duration_s`        | Duration of the segment in seconds                                                                |
| `objects_per_op`    | Objects per operation                                                                             |
| `bytes`             | Total bytes of operations (*distributed*)                                                         |
| `full_ops`          | Operations completely contained within segment                                                    |
| `partial_ops`       | Operations that either started or ended outside the segment, but was also executed during segment |
| `ops_started`       | Operations started within segment                                                                 |
| `ops_ended`         | Operations ended within the segment                                                               |
| `errors`            | Errors logged on operations ending within the segment                                             |
| `mb_per_sec`        | MiB/s of operations within the segment (*distributed*)                                             |
| `ops_ended_per_sec` | Operations that ended within the segment per second                                               |
| `objs_per_sec`      | Objects per second processed in the segment (*distributed*)                                       |
| `start_time`        | Absolute start time of the segment                                                                |
| `end_time`          | Absolute end time of the segment                                                                  |

Some of these fields are *distributed*. 
This means that the data of partial operations have been distributed across the segments they occur in. 
The bigger a percentage of the operation is within a segment the larger part of it has been attributed there.

This is why there can be a partial object attributed to a segment, 
because only a part of the operation took place in the segment.

## Comparing Benchmarks

It is possible to compare two recorded runs using the `warp cmp (file-before) (file-after)` to
see the differences between before and after.
There is no need for 'before' to be chronologically before 'after', but the differences will be shown
as change from 'before' to 'after'.

An example:
```
λ warp cmp warp-get-2019-11-29[125341]-7ylR.csv.zst warp-get-2019-202011-29[124533]-HOhm.csv.zst
-------------------
Operation: PUT
Duration: 1m4s -> 1m2s
* Average: +2.63% (+1.0 MiB/s) throughput, +2.63% (+1.0) obj/s
* Fastest: -4.51% (-4.1) obj/s
* 50% Median: +3.11% (+1.1 MiB/s) throughput, +3.11% (+1.1) obj/s
* Slowest: +1.66% (+0.4 MiB/s) throughput, +1.66% (+0.4) obj/s
-------------------
Operation: GET
Operations: 16768 -> 171105
Duration: 30s -> 5m0s
* Average: +2.10% (+11.7 MiB/s) throughput, +2.10% (+11.7) obj/s
* First Byte: Average: -405.876µs (-2%), Median: -2.1µs (-0%), Best: -998.1µs (-50%), Worst: +41.0014ms (+65%)
* Fastest: +2.35% (+14.0 MiB/s) throughput, +2.35% (+14.0) obj/s
* 50% Median: +2.81% (+15.8 MiB/s) throughput, +2.81% (+15.8) obj/s
* Slowest: -10.02% (-52.0) obj/s
```

All relevant differences are listed. This is two `warp get` runs.
Differences in parameters will be shown.

The usual analysis parameters can be applied to define segment lengths.

## Merging Benchmarks

It is possible to merge runs from several clients using the `warp merge (file1) (file2) [additional files...]` command.

The command will output a combined data file with all data that overlap in time.

The combined output will effectively be the same as having run a single benchmark with a higher concurrency setting.
The main reason for running the benchmark on several clients would be to help eliminate client bottlenecks.

It is important to note that only data that strictly overlaps in absolute time will be considered for analysis.

# Server Profiling

When running against a MinIO server it is possible to enable profiling while the benchmark is running.

This is done by adding `--serverprof=type` parameter with the type of profile you would like. 
This requires that the credentials allows admin access for the first host.

| Type  | Description                                                                                                                                |
|-------|--------------------------------------------------------------------------------------------------------------------------------------------|
| cpu   | CPU profile determines where a program spends its time while actively consuming CPU cycles (as opposed while sleeping or waiting for I/O). |
| mem   | Heap profile reports the currently live allocations; used to monitor current memory usage or check for memory leaks.                       |
| block | Block profile show where goroutines block waiting on synchronization primitives (including timer channels).                                |
| mutex | Mutex profile reports the lock contentions. When you think your CPU is not fully utilized due to a mutex contention, use this profile.     |
| trace | A detailed trace of execution of the current program. This will include information about goroutine scheduling and garbage collection.     |

Profiles for all cluster members will be downloaded as a zip file. 

Analyzing the profiles requires the Go tools to be installed. 
See [Profiling Go Programs](https://blog.golang.org/profiling-go-programs) for basic usage of the profile tools 
and an introduction to the [Go execution tracer](https://blog.gopheracademy.com/advent-2017/go-execution-tracer/) 
for more information.

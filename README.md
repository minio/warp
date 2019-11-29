
![warp](https://user-images.githubusercontent.com/5663952/69431113-580f9500-0d37-11ea-8265-46142e7b15cc.png)

S3 benchmarking tool. 

# configuration

Warp can be configured either using commandline parameters or environment variables. 

The S3 server to use can be specified on the commandline using `-host`, `-access-key`, `-secret-key` and optionally `-tls` to specify TLS.

It is also possible to set the same parameters using the `WARP_HOST`, `WARP_ACCESS_KEY`, `WARP_SECRET_KEY` and `WARP_TLS` environment variables.

Multiple hosts can be specified as comma-separated values, for instance `10.0.0.1:9000,10.0.0.2:9000,10.0.0.3:9000` 
will do a round-robin between the specified servers.
 
The credentials must be able to create, delete and list buckets and upload files and perform the operation requested.

By default operations are performed on a bucket called `warp-benchmark-bucket`.
This can be changed using the `-bucket` parameter. 
Do however note that the bucket will be completely cleaned before and after each run, 
so it should *not* contain any data.

If you are running TLS, you can enable server-side-encryption of objects using `-encrypt`. 
A random key will be generated and used.

# usage

`warp command [options]`

# benchmarks

All benchmarks operate concurrently. 
By default the processor determines the number of operations that will be running concurrently.
This can however also be tweaked using the `-concurrent` parameter.

Tweaking concurrency can have an impact on performance, especially if there is latency to the server tested.

Most benchmarks will also use different prefixes for each "thread" running. This can also

By default all benchmarks save all request details to a file named `warp-operation-yyyy-mm-dd[hhmmss].csv.zst`.
A custom file name can be specified using the `-benchdata` parameter.
The raw data is [zstandard](https://facebook.github.io/zstd/) compressed CSV data.

## get

Benchmarking get operations will upload `-objects` objects of size `-obj.size` and attempt to 
download as many it can within `-duration`. 

Objects will be uploaded with `-concurrent` different prefixes, except if `-noprefix` is specified. 
Downloads are chosen randomly between all uploaded data.

When downloading, the benchmark will attempt to run `-concurrent` concurrent downloads.

The analysis will include the upload stats as `PUT` operations and the `GET` operations.

```
Operation: GET
* Average: 2344.50 MB/s, 234.45 obj/s, 234.44 ops ended/s (59.119s)
* First Byte: Average: 1.467052ms, Median: 1.001ms, Best: 0s, Worst: 22.9984ms

Aggregated, split into 59 x 1s time segments:
* Fastest: 2693.83 MB/s, 269.38 obj/s, 269.00 ops ended/s (1s)
* 50% Median: 2419.56 MB/s, 241.96 obj/s, 240.00 ops ended/s (1s)
* Slowest: 1137.36 MB/s, 113.74 obj/s, 112.00 ops ended/s (1s)
```

The `GET` operations will contain the time until the first byte was received.

## put

Benchmarking put operations will upload objects of size `-obj.size` until `-duration` time has elapsed. 

Objects will be uploaded with `-concurrent` different prefixes, except if `-noprefix` is specified. 

```
Operation: PUT
* Average: 971.75 MB/s, 97.18 obj/s, 97.16 ops ended/s (59.417s)

Aggregated, split into 59 x 1s time segments:
* Fastest: 1591.40 MB/s, 159.14 obj/s, 161.00 ops ended/s (1s)
* 50% Median: 919.79 MB/s, 91.98 obj/s, 95.00 ops ended/s (1s)
* Slowest: 347.95 MB/s, 34.80 obj/s, 32.00 ops ended/s (1s)
```

## delete

Benchmarking delete operations will upload `-objects` objects of size `-obj.size` and attempt to 
delete as many it can within `-duration`. 

The delete operations are done in `-batch` objects per request in `-concurrent` concurrently running requests.

If there are no more objects left the benchmark will end.  

The analysis will include the upload stats as `PUT` operations and the `DELETE` operations.

```
Operation: DELETE 100 objects per operation
* Average: 2520.27 obj/s, 25.03 ops ended/s (38.554s)

Aggregated, split into 38 x 1s time segments:
* Fastest: 2955.85 obj/s, 36.00 ops ended/s (1s)
* 50% Median: 2538.10 obj/s, 25.00 ops ended/s (1s)
* Slowest: 1919.86 obj/s, 23.00 ops ended/s (1s)
```

## list

Benchmarking list operations will upload `-objects` objects of size `-obj.size` with `-concurrent` prefixes.

The list operations are done per prefix.

The analysis will include the upload stats as `PUT` operations and the `LIST` operations separately.
The time from request start to first object is recorded as well.

```
Operation: LIST 833 objects per operation
* Average: 30991.05 obj/s, 37.10 ops ended/s (59.387s)
* First Byte: Average: 322.013799ms, Median: 322.9992ms, Best: 278.0002ms, Worst: 394.0013ms

Aggregated, split into 59 x 1s time segments:
* Fastest: 31831.96 obj/s, 39.00 ops ended/s (1s)
* 50% Median: 31199.61 obj/s, 38.00 ops ended/s (1s)
* Slowest: 27917.33 obj/s, 35.00 ops ended/s (1s)
```

# Analysis

When benchmarks have finished all request data will be saved to a file and an analysis will be shown.

The saved data can be re-evaluated by running `warp analyze (filename)`. 

It is possible to merge analyses from concurrent runs using the `warp merge file1 file2 ...`.
This will combine the data as if it was run on the same client. 
Only the time segments that was actually overlapping will be considered.
This is based on the absolute time of each recording, 
so be sure that clocks are reasonably synchronized.  

## Analysis data

All analysis will be done on a reduced part of the full data. 
The data aggregation will *start* when all threads have completed one request and 
the time segment will *stop* when the last request of a thread is initiated.

This is to exclude variations due to warm-up and threads finishing at different times.
Therefore the analysis time will typically be slightly below the selected benchmark duration.

In this run "only" 42.9 seconds are included in the aggregate data, due to big payload size and low throughput:
```
Operation: PUT
* Average: 37.19 MB/s, 0.37 obj/s, 0.33 ops ended/s (42.957s)
```

The benchmark run is then divided into fixed duration *segments* specified by `-analyze.dur`, default 1s. 
For each segment the throughput is calculated across all threads.

The analysis output will display the fastest, slowest and 50% median segment.

```
Aggregated, split into 59 x 1s time segments:
* Fastest: 2693.83 MB/s, 269.38 obj/s, 269.00 ops ended/s (1s)
* 50% Median: 2419.56 MB/s, 241.96 obj/s, 240.00 ops ended/s (1s)
* Slowest: 1137.36 MB/s, 113.74 obj/s, 112.00 ops ended/s (1s)
```

### CSV output

It is possible to output the CSV data of analysis using `-analyze.out=filename.csv` 
which will write the CSV data to the specified file. 

These are the data fields exported:

| Header              | Description |
|---------------------|-------------|
| `index`             | Index of the segment  |
| `op`                | Operation executed  |
| `duration_s`        | Duration of the segment in seconds  |
| `objects_per_op`    | Objects per operation  |
| `bytes`             | Total bytes of operations (*distributed)  |
| `full_ops`          | Operations completely contained within segment  |
| `partial_ops`       | Operations that either started or ended outside the segment, but was also executed during segment  |
| `ops_started`       | Operations started within segment  |
| `ops_ended`         | Operations ended within the segment  |
| `errors`            | Errors logged on operations ending within the segment  |
| `mb_per_sec`        | MB/s of operations within the segment (*distributed)  |
| `ops_ended_per_sec` | Operations that ended within the segment per second  |
| `objs_per_sec`      | Objects per second processed in the segment (*distributed)  |
| `start_time`        | Absolute start time of the segment  |
| `end_time`          | Absolute end time of the segment  |

Some of these fields are *distributed*. 
This means that the data of partial operations have been distributed across the segments they occur in.
The bigger a percentage of the operation is within a segment the larger part of it has been attributed there.

This is why there can be a partial object attributed to a segment, because only a part of the operation took place in the segment.


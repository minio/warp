# Running *warp* on kubernetes

This document describes with simple examples on how to automate running *warp* on Kubernetes with `yaml` files. You can also use [Warp Helm Chart](./helm) to 
deploy Warp. For details on Helm chart based deployment, refer the [document here](./helm/README.md).

## Create *warp* client listeners

Create *warp* client listeners to run distributed *warp* benchmark, here we will run them as stateful sets across client nodes.

```
~ kubectl create -f warp.yaml
```

```
~ kubectl get pods -l app=warp
NAME     READY   STATUS    RESTARTS   AGE
warp-0   1/1     Running   0          6m53s
warp-1   1/1     Running   0          6m57s
warp-2   1/1     Running   0          7m8s
warp-3   1/1     Running   0          7m17s
```

Now prepare your *warp-job.yaml* (we have included a sample please edit for your needs) to benchmark your MinIO cluster
```
~ kubectl create -f warp-job.yaml
```

```
~ kubectl get pods -l job-name=warp-job
NAME             READY   STATUS      RESTARTS   AGE
warp-job-6xt5k   0/1     Completed   0          8m53s
```

To obtain the console output look at the job logs
```
~ kubectl logs warp-job-6xt5k
...
...
-------------------
Operation: PUT. Concurrency: 256. Hosts: 4.
* Average: 412.73 MiB/s, 12.90 obj/s (1m48.853s, starting 19:14:51 UTC)

Throughput by host:
 * http://minio-0.minio.default.svc.cluster.local:9000: Avg: 101.52 MiB/s, 3.17 obj/s (2m32.632s, starting 19:14:30 UTC)
 * http://minio-1.minio.default.svc.cluster.local:9000: Avg: 103.82 MiB/s, 3.24 obj/s (2m32.654s, starting 19:14:30 UTC)
 * http://minio-2.minio.default.svc.cluster.local:9000: Avg: 103.39 MiB/s, 3.23 obj/s (2m32.635s, starting 19:14:30 UTC)
 * http://minio-3.minio.default.svc.cluster.local:9000: Avg: 105.31 MiB/s, 3.29 obj/s (2m32.636s, starting 19:14:30 UTC)

Aggregated Throughput, split into 108 x 1s time segments:
 * Fastest: 677.1MiB/s, 21.16 obj/s (1s, starting 19:15:54 UTC)
 * 50% Median: 406.4MiB/s, 12.70 obj/s (1s, starting 19:14:51 UTC)
 * Slowest: 371.5MiB/s, 11.61 obj/s (1s, starting 19:15:42 UTC)
-------------------
Operation: GET. Concurrency: 256. Hosts: 4.
* Average: 866.56 MiB/s, 27.08 obj/s (4m28.204s, starting 19:17:30 UTC)

Throughput by host:
 * http://minio-0.minio.default.svc.cluster.local:9000: Avg: 180.39 MiB/s, 5.64 obj/s (4m59.817s, starting 19:17:12 UTC)
 * http://minio-1.minio.default.svc.cluster.local:9000: Avg: 179.02 MiB/s, 5.59 obj/s (4m59.24s, starting 19:17:12 UTC)
 * http://minio-2.minio.default.svc.cluster.local:9000: Avg: 182.98 MiB/s, 5.72 obj/s (4m59.697s, starting 19:17:12 UTC)
 * http://minio-3.minio.default.svc.cluster.local:9000: Avg: 322.47 MiB/s, 10.08 obj/s (4m59.929s, starting 19:17:12 UTC)

Aggregated Throughput, split into 268 x 1s time segments:
 * Fastest: 881.9MiB/s, 27.56 obj/s (1s, starting 19:20:49 UTC)
 * 50% Median: 866.4MiB/s, 27.08 obj/s (1s, starting 19:18:46 UTC)
 * Slowest: 851.4MiB/s, 26.61 obj/s (1s, starting 19:17:37 UTC)
```

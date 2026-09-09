# Warp Helm Chart

### Introduction

This chart bootstraps Warp deployment on a [Kubernetes](http://kubernetes.io) cluster using the [Helm](https://helm.sh) package manager.

### Prerequisites

- Kubernetes 1.5+.
- Clone this repository in a local path, for example `/home/warp`.

### Configuring the Chart

The [configuration](./values.yaml) file lists the configuration parameters. If you cloned the repo to `/home/warp`, edit the `/home/warp/k8s/helm/values.yaml` file to configure MinIO Server endpoint, credentials and other relevant fields explained in the [Warp documentation](https://github.com/minio/warp#usage).

We recommend setting `replicaCount` as the same number of MinIO Pods.

#### Configuration Methods

The chart supports two configuration methods:

1. **Traditional Configuration** (Simple): Use the `warpConfiguration` section in `values.yaml` for basic setup
2. **YAML Configuration File** (Advanced): Use the `configFile` option to provide a complete Warp YAML configuration

For detailed information about both methods, see the [Configuration Guide](./CONFIG.md).

### S3 over RDMA

Set `rdma.enabled` to run the benchmark over RDMA instead of HTTP. The chart
then switches both the server Job and the client StatefulSet onto warp's RDMA
image, passes the mode through to warp, and adds `CAP_IPC_LOCK` so the NIC can
pin the buffers it registers.

```yaml
image:
  # There is no rolling latest.rdma tag, so name a release. The chart appends
  # the .rdma suffix and switches to quay.io/minio/aistor/warp.
  version: v1.6.1

rdma:
  enabled: true
  mode: cpu          # or "gpu" for GPU-Direct
  resources:
    limits:
      rdma/hca: 1    # see "Reaching the fabric" below
```

`k8s/helm/values-rdma-example.yaml` is a complete example. See
[RDMA.md](../../RDMA.md) for what S3 over RDMA is and what it needs from a host.

#### Reaching the fabric

The chart configures warp; it does not give a pod an RDMA device. Kubernetes
offers three ways to do that, and `values.yaml` has a knob for each:

| Cluster provides | Set |
| ---------------- | --- |
| RDMA shared device plugin | `rdma.resources.limits."rdma/hca": 1` |
| SR-IOV with Multus | `rdma.podAnnotations."k8s.v1.cni.cncf.io/networks": <attachment>` |
| Nothing — use the node's own devices | `rdma.hostNetwork: true` |

`rdma.hostNetwork` also sets `dnsPolicy: ClusterFirstWithHostNet`, without which
the pods could not resolve the headless service the server addresses them
through. It puts the pods on the node's ports, so schedule at most one warp pod
per node — otherwise the second one fails to bind `service.port`.

#### GPU-Direct

`rdma.mode: gpu` needs an NVIDIA GPU and driver on **every** warp pod, the
server included: warp probes the CUDA runtime while parsing flags and refuses to
start when it cannot load one. Request the GPU alongside the fabric:

```yaml
rdma:
  enabled: true
  mode: gpu
  resources:
    limits:
      rdma/hca: 1
      nvidia.com/gpu: 1
```

#### Notes

- Only the `get` and `put` benchmarks accept RDMA. Warp rejects the others at
  startup rather than reporting HTTP numbers as if they were RDMA numbers.
- Warp cannot tell you whether a transfer actually used RDMA: a setup failure
  falls back to HTTP and the operation still succeeds. Confirm from the storage
  server's S3 over RDMA counters.
- `securityContext.readOnlyRootFilesystem` is on by default, so warp cannot
  write its `warp-operation-*.csv.zst` data file (the results still print to the
  Job log), and libcufile cannot write `cufile.log` under `rdma.mode: gpu`.
  Neither is fatal. To keep the data file, give the pod a writable directory and
  point `--benchdata` at it.

### Installing the Chart

After configuring the `values.yaml` file, install this chart using:

```bash
cd /home/warp/k8s/
helm install warp helm/
```

Or use a custom configuration file:

```bash
# Using the example configuration with full YAML config
helm install warp helm/ -f helm/values-configfile-example.yaml

# Or provide your own YAML config file
helm install warp helm/ --set-file configFile=my-warp-config.yml
```

The command deploys a StatefulSet with `replicaCount` number of Warp client pods and a Job with Warp Server.

### Benchmark results

After Warp chart is successfully deployed, use the `kubectl get pods` command to find out the Pod related to Warp Job. For example:

```sh
$ kubectl get pods
NAME         READY   STATUS      RESTARTS   AGE
warp-0       1/1     Running     0          11m
warp-1       1/1     Running     0          11m
warp-2       1/1     Running     0          11m
warp-3       1/1     Running     0          11m
warp-df9cs   0/1     Completed   0          11m
```

Then, use the `kubectl logs` command to get the output from Job Pod. Here you can see the benchmark results.

```sh
$ kubectl logs warp-df9cs
....
....
....
Operation: GET. Concurrency: 4. Hosts: 4.
* Average: 448.97 MiB/s, 89.79 obj/s (4m59.883s, starting 14:01:09 UTC)

Throughput by host:
 * http://minio-1.minio.default.svc.cluster.local:9000: Avg: 112.26 MiB/s, 22.45 obj/s (4m59.834s, starting 14:01:09 UTC)
 * http://minio-2.minio.default.svc.cluster.local:9000: Avg: 112.27 MiB/s, 22.45 obj/s (4m59.797s, starting 14:01:09 UTC)
 * http://minio-3.minio.default.svc.cluster.local:9000: Avg: 112.27 MiB/s, 22.45 obj/s (4m59.938s, starting 14:01:09 UTC)
 * http://minio-0.minio.default.svc.cluster.local:9000: Avg: 112.27 MiB/s, 22.45 obj/s (4m59.934s, starting 14:01:09 UTC)

Aggregated Throughput, split into 299 x 1s time segments:
 * Fastest: 580.4MiB/s, 116.09 obj/s (1s, starting 14:01:11 UTC)
 * 50% Median: 471.9MiB/s, 94.38 obj/s (1s, starting 14:04:25 UTC)
 * Slowest: 189.4MiB/s, 37.87 obj/s (1s, starting 14:02:23 UTC)
```

# Warp Helm Chart Configuration

This Helm chart now supports two ways to configure Warp benchmarks:

## Option 1: Using `configFile` (Recommended)

The new method allows you to provide a complete YAML configuration file following the Warp YAML format:

```yaml
configFile: |
  warp:
    api: v1
    benchmark: mixed
    remote:
      region: us-east-1
      access-key: 'minio'
      secret-key: 'minio123'
      host:
        - 'minio-{0...3}.minio.default.svc.cluster.local:9000'
      tls: false
    params:
      duration: 5m
      concurrent: 16
      objects: 2500
      obj:
        size: 10MiB
```

### Benefits of Using `configFile`

- **Full Feature Access**: Access to all Warp configuration options available in the YAML format
- **Direct Mapping**: Uses the same format as Warp's native YAML configuration files
- **Complex Configurations**: Better support for advanced features like:
  - Multiple hosts configuration
  - Analysis settings
  - IO customization
  - Advanced networking options
  - Auto-termination settings

### How It Works

1. When `configFile` is provided, the Helm chart:
   - Creates a ConfigMap with your YAML configuration
   - Automatically injects the `warp-client` property with the correct pod addresses based on `replicaCount`
   - Mounts it as `/config/warp-config.yml` in the container
   - Runs warp with `warp run /config/warp-config.yml` command

2. If both `configFile` and `warpConfiguration` are defined, `configFile` takes precedence

3. The `warp-client` property is automatically generated as:
   ```
   warp-client: <release-name>-{0...<replicaCount-1>}.<release-name>.<namespace>
   ```
   You don't need to specify this in your config file as it's added automatically

### Example Usage

Install the chart with a custom config file:

```bash
helm install my-warp ./k8s/helm -f values-configfile-example.yaml
```

Or provide the config inline:

```bash
helm install my-warp ./k8s/helm --set-file configFile=my-warp-config.yml
```

### Sample Configuration Files

For complete examples of YAML configuration files, see:
- [Warp YAML Samples](https://github.com/minio/warp/tree/master/yml-samples)
- `values-configfile-example.yaml` in this directory

### Migration Guide

To migrate from `warpConfiguration` to `configFile`:

1. Take your existing `warpConfiguration` values
2. Map them to the corresponding fields in the YAML format
3. Add any additional configuration options you need
4. Set the `configFile` value in your values.yaml

Example mapping:
- `warpConfiguration.s3ServerURL` → `warp.remote.host`
- `warpConfiguration.s3AccessKey` → `warp.remote.access-key`
- `warpConfiguration.s3SecretKey` → `warp.remote.secret-key`
- `warpConfiguration.operationToBenchmark` → `warp.benchmark`
- `warpJobArgs.duration` → `warp.params.duration`
- `warpJobArgs.objects` → `warp.params.objects`

## Option 2: Using `warpConfiguration` (Legacy Method)

The traditional method uses individual configuration fields in `values.yaml`, simpler for basic setups where the hostname is compatible with elipsis notation:

```yaml
warpConfiguration:
  s3ServerURL: minio-{0...3}.minio.default.svc.cluster.local:9000
  s3ServerTLSEnabled: false
  s3ServerRegion: "us-east-1"
  s3AccessKey: "minio"
  s3SecretKey: "minio123"
  operationToBenchmark: get

warpJobArgs:
  objects: 1000
  obj.size: 10MiB
  duration: 5m0s
  concurrent: 10
```

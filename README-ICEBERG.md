# Warp Iceberg Benchmark

Benchmark Iceberg table write performance by uploading parquet files to S3 storage and committing them to an Iceberg table via REST catalog.

## Overview

The `warp iceberg` command tests:
- Parquet file upload performance to S3
- Iceberg commit throughput via REST catalog
- Commit conflict handling under concurrent load

## Requirements

- Iceberg REST catalog (e.g., MinIO AIStor with Iceberg support)
- S3-compatible storage endpoint
- Warehouse must be created beforehand using `mc` or MinIO Console

## Quick Start

```bash
warp iceberg \
  --host=localhost:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --warehouse=my-warehouse \
  --duration=1m
```

## Benchmark Modes

### 1. Generated Data (Default)

Creates synthetic parquet files with configurable rows and file counts.

```bash
warp iceberg \
  --host=minio:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --warehouse=my-warehouse \
  --num-files=10 \
  --rows-per-file=10000 \
  --duration=5m
```

### 2. TPC-DS Data

Uses real TPC-DS benchmark data auto-downloaded from Google Cloud Storage.

```bash
warp iceberg \
  --host=minio:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --warehouse=my-warehouse \
  --tpcds \
  --scale-factor=sf100 \
  --duration=5m
```

## Workflow

1. Creates bucket (if needed)
2. Downloads TPC-DS data from GCS OR generates synthetic parquet files
3. Creates Iceberg namespace and table (if needed)
4. Runs upload+commit cycles until duration expires
5. Handles commit conflicts with exponential backoff retry
6. Cleans up uploaded data

## Flags Reference

### Iceberg Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--catalog-uri` | `http://<host>/_iceberg` | Iceberg REST catalog URI |
| `--warehouse` | (required) | Warehouse name (also used as bucket) |
| `--namespace` | `benchmark` | Iceberg namespace |
| `--table` | `warp_benchmark` | Iceberg table name |

### Data Generation

| Flag | Default | Description |
|------|---------|-------------|
| `--num-files` | `10` | Parquet files to generate per worker |
| `--rows-per-file` | `10000` | Rows per parquet file |

### TPC-DS Mode

| Flag | Default | Description |
|------|---------|-------------|
| `--tpcds` | `false` | Use TPC-DS data from GCS |
| `--scale-factor` | `sf100` | Scale: `sf1`, `sf10`, `sf100`, `sf1000` |
| `--tpcds-table` | `store_sales` | TPC-DS table to use |

### Commit Retry

| Flag | Default | Description |
|------|---------|-------------|
| `--max-retries` | `10` | Max commit retries on conflict |
| `--backoff-base` | `100ms` | Initial retry backoff (exponential) |

### S3 Connection

| Flag | Environment Variable | Description |
|------|---------------------|-------------|
| `--host` | `WARP_HOST` | S3 server host |
| `--access-key` | `WARP_ACCESS_KEY` | S3 access key |
| `--secret-key` | `WARP_SECRET_KEY` | S3 secret key |
| `--tls` | `WARP_TLS` | Use TLS (HTTPS) |

### Benchmark Control

| Flag | Default | Description |
|------|---------|-------------|
| `--concurrent` | `20` | Concurrent workers |
| `--duration` | `5m` | Benchmark duration |
| `--autoterm` | `false` | Auto-terminate when stable |

## Operation Types

### UPLOAD
Time to upload individual parquet files to S3 storage.

### COMMIT
Time to commit file references to the Iceberg table via REST catalog.

## Output

The benchmark outputs:
- Upload throughput (MiB/s, obj/s)
- Commit throughput (commits/s)
- Latency percentiles (p50, p90, p99)
- Error counts
- Retry statistics

Example:
```
=== Benchmark Summary ===
Total Uploads:       658
Total Commits:       328
Total Retries:       142
Total Failed Commits: 6
```

Benchmark data is saved to `.csv.zst` for later analysis:

```bash
warp analyze warp-iceberg-2024-01-15[123456]-xxxx.csv.zst
warp cmp before.csv.zst after.csv.zst
```

## Distributed Benchmarking

Run across multiple machines for higher load:

```bash
# On client machines
warp client

# On coordinator
warp iceberg \
  --host=minio:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --warehouse=my-warehouse \
  --warp-client=node1:7761,node2:7761,node3:7761 \
  --tpcds \
  --duration=5m
```

All clients commit to the same Iceberg table, maximizing commit contention.

## TPC-DS Scale Factors

| Scale Factor | Approximate Size |
|--------------|-----------------|
| `sf1` | ~1 GB |
| `sf10` | ~10 GB |
| `sf100` | ~100 GB |
| `sf1000` | ~1 TB |

## Commit Conflict Handling

When multiple workers commit simultaneously, conflicts occur. The benchmark handles these with exponential backoff:

- Retry 1: wait ~100ms
- Retry 2: wait ~200ms
- Retry 3: wait ~400ms
- ... up to 5s max

This simulates real-world concurrent write scenarios.

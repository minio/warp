# Warp Iceberg Benchmark

Benchmark Iceberg table write performance by uploading parquet files to S3 storage and committing them to an Iceberg table via REST catalog.

## Overview

The `warp iceberg` command tests:
- Parquet file upload performance to S3
- Iceberg commit throughput via REST catalog
- Commit conflict handling under concurrent load with exponential backoff retry

## Requirements

- Iceberg REST catalog (e.g., MinIO AIStor with Iceberg support)
- S3-compatible storage endpoint
- Warehouse must be created beforehand using `mc` or MinIO Console

## AIStor Architecture: Warehouse = Bucket

In MinIO AIStor, **warehouse and bucket are the same thing**. A warehouse IS a bucket - it's the physical S3 bucket that contains all tables, their metadata, and data files.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           MinIO AIStor ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   WAREHOUSE = BUCKET (they are the same)                                        │
│   ──────────────────────────────────────                                        │
│                                                                                 │
│   s3://my-warehouse/    ◄── This IS the warehouse (--warehouse & --bucket)      │
│   │                                                                             │
│   ├── {table-uuid-1}/                    # Table 1 (e.g., warp_benchmark)       │
│   │   ├── metadata/                      # Iceberg metadata for this table      │
│   │   │   ├── v00000-{uuid}.metadata.json                                       │
│   │   │   ├── v00001-{uuid}.metadata.json                                       │
│   │   │   └── v00002-{uuid}.metadata.json                                       │
│   │   └── data/                          # Parquet data files                   │
│   │       ├── part-00000-{uuid}.parquet                                         │
│   │       ├── part-00001-{uuid}.parquet                                         │
│   │       └── ...                                                               │
│   │                                                                             │
│   ├── {table-uuid-2}/                    # Table 2                              │
│   │   ├── metadata/                                                             │
│   │   └── data/                                                                 │
│   │                                                                             │
│   └── {table-uuid-N}/                    # Table N                              │
│       ├── metadata/                                                             │
│       └── data/                                                                 │
│                                                                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   CATALOG METADATA (stored in .minio.sys - internal)                            │
│   ─────────────────────────────────────────────────                             │
│                                                                                 │
│   .minio.sys/catalog/                                                           │
│   ├── warehouse.bin                      # Registry of all warehouses           │
│   └── my-warehouse/                      # Catalog data for this warehouse      │
│       ├── namespace.bin                  # Namespace registry                   │
│       ├── namespaces/                                                           │
│       │   └── benchmark/                 # Namespace (--namespace)              │
│       │       └── table-registry-shard-*.bin  # Table entries (16 shards)       │
│       └── tables/                                                               │
│           └── {table-uuid}.bin           # Metadata pointer → current version   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### `--warehouse` vs `--bucket`

| Flag | Purpose | Example |
|------|---------|---------|
| `--warehouse` | Warehouse name (used by Iceberg REST catalog API) | `my-warehouse` |
| `--bucket` | S3 bucket name (where data is physically stored) | `my-warehouse` |

**In AIStor, these should always be the same value** because:
- A warehouse IS a bucket
- Each table gets a UUID subdirectory inside the warehouse bucket
- Metadata and data for each table live under `{warehouse}/{table-uuid}/`

### Why Both Flags Exist

The `--bucket` flag is inherited from warp's common S3 flags (used by all benchmarks). The `--warehouse` flag is Iceberg-specific. For AIStor compatibility, they should match.

```bash
# Correct usage for AIStor
warp iceberg \
  --warehouse=my-warehouse \
  --bucket=my-warehouse      # Same value!
```

Since `--warehouse` defaults to `--bucket` if not specified, you can simply use:

```bash
# Simplified - warehouse auto-set to bucket value
warp iceberg --bucket=my-warehouse
```

### Table Structure Inside Warehouse

When warp creates/uses a table, AIStor assigns it a UUID. All data lives under that UUID:

```
s3://my-warehouse/{table-uuid}/
├── metadata/
│   └── v00000-{uuid}.metadata.json    # Schema, partitions, snapshots
└── data/
    └── *.parquet                       # Actual data files
```

### Warp Upload Path

Warp uploads benchmark parquet files to the correct Iceberg table data directory:

```
s3://{warehouse}/{table-uuid}/data/worker-{N}/iter-{X}/{timestamp}/{filename}.parquet
```

This ensures files are properly located within the table's data directory and can be committed to the Iceberg catalog.

## Benchmark Modes

### 1. Generated Data (Default)

Creates synthetic parquet files with configurable rows and file counts.

```bash
warp iceberg --host=minio:9000 --access-key=minioadmin --secret-key=minioadmin \
  --catalog-uri=http://minio:9000/_iceberg --warehouse=my-warehouse \
  --bucket=my-warehouse --num-files=10 --rows-per-file=10000 --iterations=5
```

### 2. TPC-DS Data

Uses real TPC-DS benchmark data auto-downloaded from Google Cloud Storage.

```bash
warp iceberg --host=minio:9000 --access-key=minioadmin --secret-key=minioadmin \
  --catalog-uri=http://minio:9000/_iceberg --warehouse=my-warehouse \
  --bucket=my-warehouse --tpcds --scale-factor=sf100 --tpcds-table=store_sales
```

## Workflow

1. Creates bucket (if needed)
2. Downloads TPC-DS data from GCS OR generates synthetic parquet files
3. Creates Iceberg namespace and table (if needed)
4. Uploads parquet files to S3 storage
5. Commits file references to Iceberg table via REST catalog
6. Handles commit conflicts with exponential backoff retry

## Flags Reference

### Iceberg Configuration

All Iceberg-specific flags are **optional** with sensible defaults:

| Flag | Required | Default | Environment Variable | Description |
|------|----------|---------|---------------------|-------------|
| `--catalog-uri` | No | Auto: `<scheme>://<host>/_iceberg` | `ICEBERG_CATALOG_URI` | Iceberg REST catalog URI. Auto-constructed from `--host` and `--tls` if not specified |
| `--warehouse` | No | Falls back to `--bucket` | `ICEBERG_WAREHOUSE` | Iceberg warehouse name or location |
| `--namespace` | No | `benchmark` | - | Iceberg namespace for the benchmark table |
| `--table` | No | `warp_benchmark` | - | Iceberg table name |

**Note:** The warehouse must exist beforehand (create via `mc` or MinIO Console). The namespace and table are auto-created if they don't exist.

### Data Generation (Non-TPC-DS Mode)

| Flag | Default | Description |
|------|---------|-------------|
| `--num-files` | `10` | Number of parquet files to generate per worker |
| `--rows-per-file` | `10000` | Number of rows per parquet file |

### TPC-DS Mode

| Flag | Default | Description |
|------|---------|-------------|
| `--tpcds` | `false` | Enable TPC-DS mode (downloads data from GCS if not cached) |
| `--scale-factor` | `sf100` | TPC-DS scale factor: `sf1`, `sf10`, `sf100`, `sf1000` |
| `--tpcds-table` | `store_sales` | TPC-DS table name to use |

### Benchmark Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--iterations` | `10` | Number of upload+commit iterations per worker |
| `--max-retries` | `10` | Maximum commit retries on conflict |
| `--backoff-base` | `100ms` | Base backoff duration for commit retries (exponential backoff) |
| `--cache-dir` | `/tmp/warp-iceberg-cache` | Local cache directory for generated/downloaded data |

### S3 Connection (Common Flags)

These are the **required** flags (unless set via environment variables):

| Flag | Required | Default | Environment Variable | Description |
|------|----------|---------|---------------------|-------------|
| `--host` | **Yes** | - | `WARP_HOST` | S3 server host(s), e.g., `minio:9000` |
| `--access-key` | **Yes** | - | `WARP_ACCESS_KEY` | S3 access key |
| `--secret-key` | **Yes** | - | `WARP_SECRET_KEY` | S3 secret key |
| `--tls` | No | `false` | `WARP_TLS` | Use TLS (HTTPS) |
| `--region` | No | - | `WARP_REGION` | S3 region |
| `--bucket` | No | `warp-benchmark-bucket` | - | Bucket name for benchmark data |

### Benchmark Control (Common Flags)

| Flag | Default | Description |
|------|---------|-------------|
| `--concurrent` | `20` | Number of concurrent operations |
| `--duration` | `5m` | Maximum benchmark duration |
| `--autoterm` | `false` | Enable automatic termination when results stabilize |
| `--autoterm.dur` | `15s` | Minimum stable duration for auto-termination |
| `--autoterm.pct` | `7.5` | Percentage threshold for stability detection |
| `--benchdata` | (auto-generated) | Custom filename for benchmark data output |
| `--noclear` | `false` | Do not clear bucket before/after benchmark |
| `--keep-data` | `false` | Keep uploaded data after benchmark |

### Distributed Benchmarking (Common Flags)

| Flag | Default | Description |
|------|---------|-------------|
| `--warp-client` | - | Comma-separated list of warp client addresses for distributed benchmarking |
| `--syncstart` | - | Synchronized start time in `hh:mm` format |

### Analysis Flags (Common Flags)

| Flag | Default | Description |
|------|---------|-------------|
| `--analyze.dur` | `1s` | Time segment duration for analysis aggregation |
| `--analyze.op` | - | Analyze only specific operation type (e.g., `UPLOAD`, `COMMIT`) |
| `--analyze.v` | `false` | Verbose analysis with per-host and per-request statistics |
| `--analyze.out` | - | Output analysis data to CSV file |

### I/O Flags (Common Flags)

| Flag | Default | Description |
|------|---------|-------------|
| `--stress` | `false` | Stress mode - don't store request data in memory |
| `--influxdb` | - | InfluxDB connection string for real-time metrics |

## Examples

### Basic Benchmark with Generated Data

```bash
warp iceberg \
  --host=localhost:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --catalog-uri=http://localhost:9000/_iceberg \
  --warehouse=test-warehouse \
  --bucket=test-warehouse \
  --num-files=20 \
  --rows-per-file=50000 \
  --iterations=10 \
  --concurrent=4
```

### TPC-DS Benchmark

```bash
warp iceberg \
  --host=localhost:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --catalog-uri=http://localhost:9000/_iceberg \
  --warehouse=test-warehouse \
  --bucket=test-warehouse \
  --tpcds \
  --scale-factor=sf100 \
  --tpcds-table=store_sales \
  --concurrent=8
```

### Distributed Benchmark

```bash
warp iceberg \
  --host=minio-server-{1...4}:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --catalog-uri=http://minio-server-1:9000/_iceberg \
  --warehouse=my-warehouse \
  --bucket=my-warehouse \
  --warp-client=warp-client-{1...10}:7761 \
  --tpcds \
  --scale-factor=sf100
```

### With Auto-Termination

```bash
warp iceberg \
  --host=localhost:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --catalog-uri=http://localhost:9000/_iceberg \
  --warehouse=test-warehouse \
  --bucket=test-warehouse \
  --tpcds \
  --autoterm \
  --autoterm.dur=30s \
  --autoterm.pct=5
```

### With TLS

```bash
warp iceberg \
  --host=minio.example.com:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --tls \
  --catalog-uri=https://minio.example.com:9000/_iceberg \
  --warehouse=secure-warehouse \
  --bucket=secure-warehouse
```

### Custom Retry Configuration

```bash
warp iceberg \
  --host=localhost:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --catalog-uri=http://localhost:9000/_iceberg \
  --warehouse=test-warehouse \
  --bucket=test-warehouse \
  --max-retries=20 \
  --backoff-base=50ms \
  --concurrent=16
```

### With InfluxDB Metrics

```bash
warp iceberg \
  --host=localhost:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --catalog-uri=http://localhost:9000/_iceberg \
  --warehouse=test-warehouse \
  --bucket=test-warehouse \
  --tpcds \
  --influxdb="http://mytoken@influxdb:8086/warp/myorg?env=test"
```

### Verbose Analysis Output

```bash
warp iceberg \
  --host=localhost:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --catalog-uri=http://localhost:9000/_iceberg \
  --warehouse=test-warehouse \
  --bucket=test-warehouse \
  --analyze.v \
  --analyze.out=iceberg-results.csv
```

## Operation Types

The benchmark records two operation types:

### UPLOAD

Time to upload individual parquet files to S3 storage.

- Measures S3 PUT performance
- Files are uploaded with unique paths per worker and iteration
- Size reflects the parquet file size

### COMMIT

Time to commit file references to the Iceberg table.

- Measures Iceberg REST catalog commit performance
- Includes retry handling for commit conflicts
- `ObjPerOp` reflects number of files committed in a single transaction

## Output

Benchmark data is saved to a compressed CSV file (`.csv.zst`) that can be analyzed later:

```bash
# Re-analyze saved benchmark data
warp analyze warp-iceberg-2024-01-15[123456]-xxxx.csv.zst

# Compare two benchmark runs
warp cmp before.csv.zst after.csv.zst

# Merge distributed benchmark results
warp merge client1.csv.zst client2.csv.zst client3.csv.zst
```

## TPC-DS Scale Factors

| Scale Factor | Approximate Data Size |
|--------------|----------------------|
| `sf1` | ~1 GB |
| `sf10` | ~10 GB |
| `sf100` | ~100 GB |
| `sf1000` | ~1 TB |

Data is automatically downloaded from Google Cloud Storage and cached locally in `--cache-dir`.

## Commit Conflict Handling

When multiple workers attempt to commit to the same Iceberg table concurrently, commit conflicts may occur. The benchmark handles these with:

1. **Exponential backoff**: Starting from `--backoff-base`, delays double with each retry up to 5 seconds max
2. **Configurable retries**: `--max-retries` controls maximum retry attempts
3. **Statistics tracking**: Retry counts and failures are recorded in the benchmark output

This simulates real-world concurrent write scenarios and measures the system's ability to handle contention.

## Distributed Benchmarking

The iceberg benchmark fully supports distributed mode, allowing multiple warp clients to coordinate and stress-test the Iceberg catalog from multiple machines simultaneously.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              WARP SERVER                                        │
│                         (Coordinator Machine)                                   │
│                                                                                 │
│   warp iceberg --warp-client=client-{1...4}:7761 --host=minio:9000 ...         │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
          │                    │                    │                    │
          │ Sends config       │ Sends config       │ Sends config       │ Sends config
          │ & coordinates      │ & coordinates      │ & coordinates      │ & coordinates
          ▼                    ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  WARP CLIENT 1  │  │  WARP CLIENT 2  │  │  WARP CLIENT 3  │  │  WARP CLIENT 4  │
│  (client-1)     │  │  (client-2)     │  │  (client-3)     │  │  (client-4)     │
│                 │  │                 │  │                 │  │                 │
│  warp client    │  │  warp client    │  │  warp client    │  │  warp client    │
│  :7761          │  │  :7761          │  │  :7761          │  │  :7761          │
└─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘
          │                    │                    │                    │
          │                    │                    │                    │
          ▼                    ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                                                                 │
│                         MinIO / S3 Storage + Iceberg Catalog                    │
│                                                                                 │
│   ┌─────────────────────────────┐    ┌─────────────────────────────────────┐   │
│   │      S3 Bucket              │    │      Iceberg REST Catalog           │   │
│   │  (parquet file storage)     │    │   (table metadata & commits)        │   │
│   └─────────────────────────────┘    └─────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Setup

#### Step 1: Start Warp Clients

On each client machine, start the warp client listener:

```bash
# On client-1
warp client client-1:7761

# On client-2
warp client client-2:7761

# On client-3
warp client client-3:7761

# On client-4
warp client client-4:7761
```

By default, `warp client` listens on `127.0.0.1:7761`. Specify the address to listen on all interfaces.

#### Step 2: Run Distributed Benchmark from Server

From the coordinator machine, run the benchmark with `--warp-client`:

```bash
warp iceberg \
  --host=minio-server:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --catalog-uri=http://minio-server:9000/_iceberg \
  --warehouse=my-warehouse \
  --bucket=my-warehouse \
  --warp-client=client-{1...4}:7761 \
  --tpcds \
  --scale-factor=sf100 \
  --concurrent=8 \
  --iterations=20
```

### How Distributed Mode Works

```
Timeline:
─────────────────────────────────────────────────────────────────────────────────

Server                Client-1              Client-2              Client-3
──────                ────────              ────────              ────────
   │                     │                     │                     │
   │── Send config ─────►│                     │                     │
   │── Send config ──────┼────────────────────►│                     │
   │── Send config ──────┼─────────────────────┼────────────────────►│
   │                     │                     │                     │
   │                     │                     │                     │
   │   ┌─────────────────┼─────────────────────┼─────────────────────┤
   │   │                 │                     │                     │
   │   │  SYNCHRONIZED   │     Download/Generate Data (Prepare)      │
   │   │     START       │                     │                     │
   │   │                 │                     │                     │
   │   └─────────────────┼─────────────────────┼─────────────────────┤
   │                     │                     │                     │
   │                     ▼                     ▼                     ▼
   │               ┌──────────┐          ┌──────────┐          ┌──────────┐
   │               │ Worker 0 │          │ Worker 0 │          │ Worker 0 │
   │               │ Worker 1 │          │ Worker 1 │          │ Worker 1 │
   │               │ Worker 2 │          │ Worker 2 │          │ Worker 2 │
   │               │   ...    │          │   ...    │          │   ...    │
   │               │ Worker N │          │ Worker N │          │ Worker N │
   │               └──────────┘          └──────────┘          └──────────┘
   │                     │                     │                     │
   │                     │   UPLOAD + COMMIT   │   UPLOAD + COMMIT   │
   │                     │   to same Iceberg   │   to same Iceberg   │
   │                     │       table         │       table         │
   │                     │                     │                     │
   │                     ▼                     ▼                     ▼
   │               ┌─────────────────────────────────────────────────────┐
   │               │                                                     │
   │               │            Iceberg Table (shared)                   │
   │               │         Commit conflicts → Retry                    │
   │               │                                                     │
   │               └─────────────────────────────────────────────────────┘
   │                     │                     │                     │
   │◄── Results ─────────┤                     │                     │
   │◄── Results ─────────┼─────────────────────┤                     │
   │◄── Results ─────────┼─────────────────────┼─────────────────────┤
   │                     │                     │                     │
   ▼                     │                     │                     │
Merge & Display          │                     │                     │
Combined Results         │                     │                     │
```

### Distributed Mode Behavior

| Aspect | Behavior |
|--------|----------|
| **Configuration** | Server sends all benchmark parameters to clients |
| **Data Preparation** | Each client downloads/generates data independently |
| **Concurrency** | `--concurrent=N` applies to EACH client (total = N × num_clients) |
| **Iterations** | `--iterations=N` applies to EACH client |
| **Commit Conflicts** | More clients = more conflicts = better stress test |
| **Results** | Server collects, merges, and displays combined statistics |
| **Local Saves** | Each client also saves its own `.csv.zst` file locally |

### Example: 4 Clients with 8 Concurrent Workers Each

```
Settings:
  --warp-client=client-{1...4}:7761
  --concurrent=8
  --iterations=10

Result:
  Client 1: 8 workers × 10 iterations = 80 commit attempts
  Client 2: 8 workers × 10 iterations = 80 commit attempts
  Client 3: 8 workers × 10 iterations = 80 commit attempts
  Client 4: 8 workers × 10 iterations = 80 commit attempts
  ─────────────────────────────────────────────────────────
  TOTAL:    32 workers × 10 iterations = 320 commit attempts
            (all hitting the same Iceberg table simultaneously)
```

### Client Address Formats

```bash
# Comma-separated list
--warp-client=192.168.1.10:7761,192.168.1.11:7761,192.168.1.12:7761

# Range expansion
--warp-client=client-{1...10}:7761

# Range expansion with IP
--warp-client=192.168.1.{10...20}:7761

# From file (one host per line)
--warp-client=file:clients.txt
```

### Important Notes

1. **Clock Synchronization**: For reliable benchmarks, all client machines should have synchronized clocks (use NTP)

2. **Network Access**: All clients must be able to reach:
   - The S3/MinIO storage endpoint (`--host`)
   - The Iceberg REST catalog (`--catalog-uri`)

3. **Security Warning**: Never expose warp clients on public ports - they can execute benchmarks against any target

4. **Version Matching**: Keep warp versions identical across server and all clients

### Distributed Benchmark Examples

#### Basic Distributed with Generated Data

```bash
warp iceberg \
  --host=minio:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --warehouse=test-warehouse \
  --bucket=test-warehouse \
  --warp-client=client-{1...4}:7761 \
  --num-files=20 \
  --iterations=50 \
  --concurrent=16
```

#### Large Scale TPC-DS Test

```bash
warp iceberg \
  --host=minio-{1...8}:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --catalog-uri=http://minio-1:9000/_iceberg \
  --warehouse=production-warehouse \
  --bucket=production-warehouse \
  --warp-client=warp-{1...20}:7761 \
  --tpcds \
  --scale-factor=sf1000 \
  --concurrent=32 \
  --iterations=100 \
  --autoterm
```

#### With InfluxDB Monitoring

```bash
warp iceberg \
  --host=minio:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --warehouse=test-warehouse \
  --bucket=test-warehouse \
  --warp-client=client-{1...10}:7761 \
  --tpcds \
  --influxdb="http://token@influxdb.example.com:8086/warp/myorg?cluster=prod"
```

All clients will send metrics to InfluxDB, tagged with unique `warp_id` for identification.

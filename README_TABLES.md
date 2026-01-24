# Warp Tables Benchmarks

Warp provides benchmarking tools for Apache Iceberg REST catalog operations. These benchmarks test catalog metadata performance including namespace, table, and view operations.

## Overview

Four benchmark commands are available:

| Command | Description |
|---------|-------------|
| `warp tables write` | Parquet file upload and Iceberg commit performance |
| `warp tables catalog-read` | Catalog read operations (list, get, exists) |
| `warp tables catalog-commits` | Table/view property updates (commit generation) |
| `warp tables catalog-mixed` | Mixed read/write workload |

## Supported Catalogs

- **MinIO AIStor Tables** (default): Uses AWS SigV4 authentication
- **Apache Polaris**: Uses OAuth2 authentication (`--external-catalog polaris`)

## Common Flags

All tables commands share these flags:

### Connection
| Flag | Default | Description |
|------|---------|-------------|
| `--host` | (required) | Catalog server host(s), comma-separated or expandable patterns |
| `--access-key` | (required) | Access key (AWS key or OAuth client ID) |
| `--secret-key` | (required) | Secret key (AWS secret or OAuth client secret) |
| `--region` | us-east-1 | AWS region |
| `--tls` | false | Use TLS |
| `--external-catalog` | "" | External catalog type (`polaris`) |

### Tree Configuration
| Flag | Default | Description |
|------|---------|-------------|
| `--catalog-name` | benchmarkcatalog | Catalog/warehouse name |
| `--namespace-width` | varies | Width of N-ary namespace tree |
| `--namespace-depth` | varies | Depth of namespace tree |
| `--tables-per-ns` | varies | Tables per leaf namespace |
| `--views-per-ns` | varies | Views per leaf namespace |
| `--columns` | 10 | Columns per table/view schema |
| `--properties` | 5 | Properties per entity |
| `--base-location` | s3://benchmark | Base S3 location for tables |

### Benchmark Control
| Flag | Default | Description |
|------|---------|-------------|
| `--concurrent` | 20 | Number of concurrent workers |
| `--duration` | 5m | Benchmark duration |
| `--autoterm` | false | Enable auto-termination when throughput stabilizes |
| `--autoterm.dur` | 15s | Stability window for autoterm |
| `--autoterm.pct` | 7.5 | Throughput variance threshold (%) |

## Tree Structure

The `--namespace-width` and `--namespace-depth` flags define an N-ary tree of namespaces. Tables and views are created only in leaf namespaces.

### Calculations
- Total namespaces = `(width^depth - 1) / (width - 1)` for width > 1
- Leaf namespaces = `width^(depth-1)`
- Total tables = `leaf_namespaces * tables_per_ns`
- Total views = `leaf_namespaces * views_per_ns`

### Example Tree (width=2, depth=3)
```
ns_0
├── ns_1
│   ├── ns_3 (leaf: tables, views)
│   └── ns_4 (leaf: tables, views)
└── ns_2
    ├── ns_5 (leaf: tables, views)
    └── ns_6 (leaf: tables, views)
```

## Multiple Hosts / Catalog Pool

Multiple catalog hosts can be specified for load balancing:

```bash
# Comma-separated
--host=host1:9001,host2:9001,host3:9001

# Expandable pattern
--host=host{1...10}:9001
```

Requests are distributed across hosts using round-robin.

---

## TABLES WRITE

Benchmarks Iceberg table write performance by uploading parquet files to S3 and committing them to Iceberg tables.

### Usage
```bash
warp tables write [FLAGS]
```

### Workflow
1. Creates warehouse namespace/table tree structure
2. Downloads or generates parquet data files
3. Uploads parquet files to S3 storage
4. Commits file references to Iceberg tables via REST catalog
5. Handles commit conflicts with exponential backoff retry

### Additional Flags

#### Data Configuration
| Flag | Default | Description |
|------|---------|-------------|
| `--num-files` | 10 | Parquet files to generate per worker |
| `--rows-per-file` | 10000 | Rows per parquet file |
| `--cache-dir` | /tmp/warp-iceberg-cache | Local cache for data files |

#### TPC-DS Data
| Flag | Default | Description |
|------|---------|-------------|
| `--tpcds` | false | Use TPC-DS benchmark data from GCS |
| `--scale-factor` | sf100 | TPC-DS scale (sf1, sf10, sf100, sf1000) |
| `--tpcds-table` | store_sales | TPC-DS table name |

#### Retry/Conflict Handling
| Flag | Default | Description |
|------|---------|-------------|
| `--max-retries` | 4 | Maximum commit retries on conflict |
| `--backoff-base` | 100ms | Base backoff duration for retries |
| `--backoff-max` | 60s | Maximum backoff duration |

### Operations Recorded
- `UPLOAD`: Parquet file upload to S3
- `COMMIT`: Iceberg table commit with file references

### Example
```bash
# Basic write benchmark
warp tables write \
  --host=localhost:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --namespace-width=2 \
  --namespace-depth=2 \
  --tables-per-ns=5 \
  --concurrent=20 \
  --duration=1m

# With TPC-DS data
warp tables write \
  --host=localhost:9000 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --tpcds \
  --scale-factor=sf100 \
  --tpcds-table=store_sales
```

---

## TABLES CATALOG-READ

Benchmarks Iceberg REST catalog read operations.

### Usage
```bash
warp tables catalog-read [FLAGS]
```

### Workflow
1. Creates N-ary namespace tree with tables and views
2. Spawns workers that execute read operations from a shuffled pool

### Default Tree Configuration
- `--namespace-width`: 2
- `--namespace-depth`: 3
- `--tables-per-ns`: 5
- `--views-per-ns`: 5

### Operation Distribution Flags

Weights control the proportion of each operation type:

| Flag | Default | Operation |
|------|---------|-----------|
| `--ns-list-distrib` | 10 | List child namespaces |
| `--ns-head-distrib` | 10 | Check namespace exists |
| `--ns-get-distrib` | 10 | Load namespace properties |
| `--table-list-distrib` | 10 | List tables in namespace |
| `--table-head-distrib` | 10 | Check table exists |
| `--table-get-distrib` | 10 | Load table metadata |
| `--view-list-distrib` | 10 | List views in namespace |
| `--view-head-distrib` | 10 | Check view exists |
| `--view-get-distrib` | 10 | Load view metadata |

### Operations Recorded
- `NS_LIST`, `NS_HEAD`, `NS_GET`: Namespace operations
- `TABLE_LIST`, `TABLE_HEAD`, `TABLE_GET`: Table operations
- `VIEW_LIST`, `VIEW_HEAD`, `VIEW_GET`: View operations

### Example
```bash
# Default read benchmark
warp tables catalog-read \
  --host=localhost:9001 \
  --access-key=minioadmin \
  --secret-key=minioadmin

# Heavy table reads
warp tables catalog-read \
  --host=localhost:9001 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --table-get-distrib=50 \
  --table-list-distrib=20
```

---

## TABLES CATALOG-COMMITS

Benchmarks Iceberg REST catalog commit generation by updating table/view properties.

### Usage
```bash
warp tables catalog-commits [FLAGS]
```

### Workflow
1. Creates N-ary namespace tree with tables and views
2. Workers are split between table updates and view updates (default: 50/50 split of `--concurrent`)

### Default Tree Configuration
- `--namespace-width`: 2
- `--namespace-depth`: 3
- `--tables-per-ns`: 5
- `--views-per-ns`: 5

### Additional Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--table-commits-throughput` | 0 | Number of table update workers (0 = use `--concurrent/2`) |
| `--view-commits-throughput` | 0 | Number of view update workers (0 = use `--concurrent/2`) |
| `--max-retries` | 4 | Retries on 409 Conflict or 500 errors |
| `--retry-backoff` | 100ms | Initial backoff duration |
| `--backoff-max` | 60s | Maximum backoff duration |

**Note:** When you set explicit values, `--concurrent` is ignored. Total workers = `table-commits-throughput + view-commits-throughput`.

### Operations Recorded
- `TABLE_UPDATE`: Table property update
- `VIEW_UPDATE`: View property update

### Example
```bash
# Basic commit benchmark
warp tables catalog-commits \
  --host=localhost:9001 \
  --access-key=minioadmin \
  --secret-key=minioadmin

# More table commits than view commits
warp tables catalog-commits \
  --host=localhost:9001 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --table-commits-throughput=15 \
  --view-commits-throughput=5

# Tables only (no views)
warp tables catalog-commits \
  --host=localhost:9001 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --views-per-ns=0
```

---

## TABLES CATALOG-MIXED

Benchmarks mixed read/write workload with configurable operation distribution.

### Usage
```bash
warp tables catalog-mixed [FLAGS]
```

### Workflow
1. Creates N-ary namespace tree with tables and views
2. Workers execute random mix of read and update operations from shuffled pool

### Default Tree Configuration
- `--namespace-width`: 2
- `--namespace-depth`: 3
- `--tables-per-ns`: 5
- `--views-per-ns`: 5

### Operation Distribution Flags

All read operations from catalog-read plus update operations:

| Flag | Default | Operation |
|------|---------|-----------|
| `--ns-update-distrib` | 5 | Update namespace properties |
| `--table-update-distrib` | 5 | Update table properties |
| `--view-update-distrib` | 5 | Update view properties |

### Additional Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--max-retries` | 5 | Retries for update operations on conflict |
| `--retry-backoff` | 100ms | Initial backoff duration |
| `--backoff-max` | 2s | Maximum backoff duration |

### Operations Recorded
- All read operations from catalog-read
- `NS_UPDATE`, `TABLE_UPDATE`, `VIEW_UPDATE`: Update operations

### Example
```bash
# Default mixed workload
warp tables catalog-mixed \
  --host=localhost:9001 \
  --access-key=minioadmin \
  --secret-key=minioadmin

# Read-only (disable all updates)
warp tables catalog-mixed \
  --host=localhost:9001 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --ns-update-distrib=0 \
  --table-update-distrib=0 \
  --view-update-distrib=0

# Heavy writes
warp tables catalog-mixed \
  --host=localhost:9001 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --ns-update-distrib=20 \
  --table-update-distrib=20 \
  --view-update-distrib=20
```

---

## Apache Polaris Configuration

To benchmark an Apache Polaris catalog:

```bash
warp tables catalog-read \
  --host=polaris.example.com:8181 \
  --access-key=client_id \
  --secret-key=client_secret \
  --external-catalog=polaris \
  --catalog-name=warehouse \
  --base-location=s3://bucket
```

The access-key and secret-key are used as OAuth2 client credentials.

---

## Distributed Benchmarking

Tables benchmarks support distributed mode with multiple warp clients:

```bash
# Start clients on separate machines
warp client :7761

# Run distributed benchmark from server
warp tables catalog-read \
  --warp-client=client-{1...4}:7761 \
  --host=catalog-server:9001 \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --concurrent=50 \
  --duration=5m
```

In distributed mode:
- Only the first client (ClientIdx=0) creates and cleans up the dataset
- All clients participate in the benchmark phase
- Results are merged by the server

---

## Output and Analysis

Benchmark results are saved to `warp-operation-yyyy-mm-dd[hhmmss]-xxxx.csv.zst`.

```bash
# Analyze results
warp analyze warp-operation-2024-01-15[120000]-AbCd.csv.zst

# Compare two runs
warp cmp before.csv.zst after.csv.zst

# Merge results from multiple clients
warp merge client1.csv.zst client2.csv.zst
```

### Metrics Recorded

Each operation records:
- Operation type (e.g., NS_LIST, TABLE_GET)
- Start/End timestamps (nanosecond precision)
- Entity identifier (namespace path, table name)
- Error message (if failed)

### Analysis Output
- Throughput (ops/sec)
- Latency percentiles (p50, p90, p99, p99.9)
- Error counts and rates
- Per-host breakdown (with `--analyze.v`)

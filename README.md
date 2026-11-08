# Network Monitoring System with Apache Cassandra

[![Python Checks](https://github.com/arman-h-alavi/network-monitoring-cassandra/actions/workflows/python-checks.yml/badge.svg?branch=main)](https://github.com/arman-h-alavi/network-monitoring-cassandra/actions/workflows/python-checks.yml)

A Python-based time-series data pipeline that generates synthetic network infrastructure metrics, stores them in Apache Cassandra, and retrieves measurements through a command-line query tool.

## Overview

The project simulates monitoring data for 50 network resources:

| Resource type | Count |
|---|---:|
| Routers | 20 |
| Firewalls | 15 |
| Switches | 15 |
| **Total** | **50** |

Each resource is monitored using seven metrics:

- CPU Usage
- Memory Usage
- Incoming Traffic
- Outgoing Traffic
- Packets Received
- Packets Dropped
- Network Latency

Measurements are generated every five minutes over one year, from November 1, 2025, through October 31, 2026.

| Dataset characteristic | Value |
|---|---:|
| Resources | 50 |
| Metrics per resource | 7 |
| Sampling interval | 5 minutes |
| Measurements per day | 100,800 |
| Distinct resource-metric combinations | 350 |
| Total time-series records | **36,792,000** |

The dataset is synthetic and is intended for database and time-series workload experimentation, not as a representation of actual network telemetry.

## Architecture

```text
generate_dataset.py
        |
        v
    CSV Dataset
        |
        v
   ingest_data.py
        |
        v
 Apache Cassandra
        |
        v
   query_data.py
        |
        v
   Results CSV
```

| Component | Responsibility |
|---|---|
| `generate_dataset.py` | Generates reproducible synthetic network measurements. |
| `ingest_data.py` | Loads time-series measurements and resource-metric metadata into Cassandra. |
| `query_data.py` | Retrieves measurements by resource, metric, and time range, then exports results to CSV. |

## Technology Stack

- Python 3.11
- Apache Cassandra 5.0.9
- Cassandra Python Driver 3.30.1
- Docker
- pytest
- Ruff
- GitHub Actions

## Repository Structure

```text
network-monitoring-cassandra/
├── .github/
│   └── workflows/
│       └── python-checks.yml
├── data/
│   └── sample_network_monitoring.csv
├── tests/
│   ├── test_cassandra_integration.py
│   ├── test_generate_dataset.py
│   ├── test_ingest_data.py
│   └── test_query_data.py
├── .gitignore
├── README.md
├── requirements.txt
├── requirements-dev.txt
├── generate_dataset.py
├── ingest_data.py
└── query_data.py
```

The full generated dataset is excluded from version control because of its size. A 1,000-record sample is included in `data/sample_network_monitoring.csv` to illustrate the CSV structure.

## Getting Started

### Prerequisites

- Python 3.11 or later
- Docker
- Git

### 1. Clone the repository

```bash
git clone https://github.com/arman-h-alavi/network-monitoring-cassandra.git
cd network-monitoring-cassandra
```

### 2. Set up the Python environment

Create and activate a virtual environment:

```bash
python3 -m venv cassandra-env
source cassandra-env/bin/activate
```

Install runtime dependencies:

```bash
python3 -m pip install -r requirements.txt
```

For development and testing, install the additional dependencies:

```bash
python3 -m pip install -r requirements-dev.txt
```

The development requirements include the runtime dependencies, pytest, and Ruff.

### 3. Start Cassandra

Run a single-node Cassandra 5.0.9 container with a persistent Docker volume:

```bash
docker run -d \
  --name cassandra-single \
  -p 9042:9042 \
  -v cassandra_data:/var/lib/cassandra \
  cassandra:5.0.9
```

Wait for Cassandra to become ready:

```bash
docker exec cassandra-single \
  cqlsh -e "DESCRIBE KEYSPACES"
```

If Cassandra is still initializing, repeat the readiness check after a short wait.

### 4. Create the keyspace and tables

Connect to Cassandra:

```bash
docker exec -it cassandra-single cqlsh
```

Create the keyspace:

```sql
CREATE KEYSPACE IF NOT EXISTS network_monitoring
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
```

Create the metadata table:

```sql
CREATE TABLE IF NOT EXISTS network_monitoring.metrics_by_resource (
    resource TEXT,
    metric_name TEXT,
    PRIMARY KEY (resource, metric_name)
);
```

Create the time-series table:

```sql
CREATE TABLE IF NOT EXISTS network_monitoring.resource_metrics_by_period (
    resource TEXT,
    bucket_start DATE,
    metric_name TEXT,
    collected_at TIMESTAMP,
    value DOUBLE,
    PRIMARY KEY (
        (resource, bucket_start),
        metric_name,
        collected_at
    )
)
WITH CLUSTERING ORDER BY (
    metric_name ASC,
    collected_at ASC
);
```

Exit `cqlsh`:

```sql
EXIT;
```

### 5. Generate the dataset

Run:

```bash
python3 generate_dataset.py
```

The script generates `network_monitoring.csv` in the current working directory.

The default configuration generates 36,792,000 records using:

- 50 network resources.
- Seven metrics per resource.
- 365 days of measurements.
- A five-minute sampling interval.

The generator uses a fixed random seed (`42`) to make the output reproducible.

### 6. Ingest the data

Run:

```bash
python3 ingest_data.py
```

The ingestion script expects `network_monitoring.csv` in the current working directory and connects to Cassandra at `127.0.0.1:9042`.

It performs two ingestion operations:

1. Loads time-series measurements into `resource_metrics_by_period`.
2. Loads distinct resource-metric combinations into `metrics_by_resource`.

The script uses prepared statements and bounded concurrent execution to manage the ingestion workload.

## Cassandra Data Model

The database uses two tables designed around their intended access patterns.

### `metrics_by_resource`

Stores the distinct metrics associated with each network resource.

**Primary key:**

```sql
PRIMARY KEY (resource, metric_name)
```

- Partition key: `resource`
- Clustering column: `metric_name`

The table contains 350 rows: 50 resources multiplied by seven metrics.

### `resource_metrics_by_period`

Stores the time-series measurements.

**Primary key:**

```sql
PRIMARY KEY (
    (resource, bucket_start),
    metric_name,
    collected_at
)
```

- Partition key: `(resource, bucket_start)`
- Clustering columns: `metric_name`, `collected_at`

Measurements are grouped by resource and time bucket. Within each partition, records are ordered by metric name and collection timestamp.

## Time-Based Partitioning

The time-series table uses seven-day buckets anchored at November 1, 2025.

The ingestion script calculates the beginning of the relevant bucket using:

```python
days_from_anchor = (
    collected_at.date() - BUCKET_ANCHOR
).days

bucket_start = BUCKET_ANCHOR + timedelta(
    days=(days_from_anchor // 7) * 7
)
```

Each complete seven-day bucket contains:

- 7 days.
- 288 sampling intervals per day.
- 7 metrics per interval.
- **14,112 records per resource.**

The one-year dataset produces 53 buckets per resource:

- 52 complete seven-day buckets.
- One final one-day bucket.

This approach bounds partition size and supports time-range queries using the complete partition key.

## Querying Measurements

`query_data.py` provides a command-line interface for retrieving measurements.

It accepts:

- A resource ID.
- One or more metric names.
- Start and end timestamps.
- An optional output CSV filename.
- Optional Cassandra host and port arguments.

### Example

Retrieve CPU and memory measurements for a router over a two-day period:

```bash
python3 query_data.py \
  --resource RTR-001 \
  --metrics "CPU Usage,Memory Usage" \
  --start "2025-11-01 00:00" \
  --end "2025-11-03 00:00" \
  --output query_results.csv
```

The end timestamp is exclusive.

For ranges spanning multiple seven-day buckets, the script calculates the relevant bucket start dates and queries each partition separately.

The resulting CSV contains:

```text
resource,metric_name,collected_at,value
```

The Cassandra host and port can also be overridden:

```bash
python3 query_data.py \
  --resource RTR-001 \
  --metrics "CPU Usage" \
  --start "2025-11-01 00:00" \
  --end "2025-11-02 00:00" \
  --host 127.0.0.1 \
  --port 9042
```

Run the following command to see all available arguments:

```bash
python3 query_data.py --help
```

The query tool validates timestamp formats, supported metric names, duplicate metrics, and time-range boundaries.

## Data Simulation Assumptions

The generator uses resource-specific baseline profiles and introduces variation through:

- Daily workload cycles, with higher activity during daytime.
- Lower activity on weekends.
- Resource-specific traffic, CPU, and memory factors.
- Random variation in measurements.
- Workload-dependent packet drops and latency.

### Measurement Units

| Metric | Unit |
|---|---|
| CPU Usage | Percentage |
| Memory Usage | Percentage |
| Incoming Traffic | Mbps |
| Outgoing Traffic | Mbps |
| Packets Received | Estimated packet count |
| Packets Dropped | Estimated packet count |
| Network Latency | Milliseconds |

Packet counts are estimated using an assumed average packet size of 1,000 bytes. These values are simulation outputs, not observations from physical devices.

## Testing and Code Quality

The project includes 24 unit tests and a Cassandra integration test.

### Unit Tests

The unit tests cover:

- Dataset generation and reproducibility.
- Resource and measurement generation.
- Seven-day bucket calculations.
- Ingestion logic and batch execution.
- Query argument validation and timestamp handling.
- Query partition selection and result processing.

Run the unit tests:

```bash
python3 -m pytest \
  tests/test_query_data.py \
  tests/test_generate_dataset.py \
  tests/test_ingest_data.py \
  -v
```

### Cassandra Integration Test

`tests/test_cassandra_integration.py` verifies the ingestion and query pipeline against a real Cassandra instance.

It:

1. Creates an isolated `network_monitoring_test` keyspace and the required tables.
2. Generates a small temporary CSV dataset.
3. Executes time-series and metadata ingestion.
4. Queries the inserted measurements.
5. Checks the returned measurements and metadata.
6. Cleans up the test records.

A Cassandra instance must be available at `127.0.0.1:9042` by default.

The connection can be configured using the `CASSANDRA_HOST` and `CASSANDRA_PORT` environment variables.

Run the integration test:

```bash
python3 -m pytest tests/test_cassandra_integration.py -v
```

### Ruff

Ruff is used for Python linting and code-quality checks.

Run:

```bash
ruff check .
```

A successful result is:

```text
All checks passed!
```

## Continuous Integration

GitHub Actions automatically runs the workflow on pushes and pull requests.

The workflow is defined in:

```text
.github/workflows/python-checks.yml
```

It contains two independent jobs:

| Job | Checks |
|---|---|
| `code-quality` | Installs development dependencies, runs Ruff, and executes the 24 unit tests. |
| `cassandra-integration` | Starts a Cassandra 5.0.9 service and executes the integration test. |

Both jobs use Ubuntu and Python 3.11.

The Cassandra integration job uses a temporary service container, so it does not depend on a manually maintained database instance.

## Design Decisions

- **Time-series partitioning:** Seven-day buckets bound the amount of data stored in each resource partition.
- **Query-oriented schema:** Partition and clustering keys reflect the intended resource, metric, and time-range access patterns.
- **Prepared statements:** Reused CQL statements avoid repeatedly preparing identical queries.
- **Concurrent ingestion:** Bounded groups of concurrent inserts improve ingestion throughput while limiting pending statements.
- **Reproducible data generation:** A fixed random seed makes generated datasets repeatable.
- **Import-safe modules:** Script execution is separated from reusable functions, allowing components to be imported and tested independently.
- **Development dependencies:** Runtime and development requirements are maintained separately.
- **Single-node deployment:** Replication factor one and `SimpleStrategy` are used for local experimentation. This configuration does not provide production-grade fault tolerance or high availability.

# Network Monitoring System with Apache Cassandra

A Python-based time-series data pipeline that generates synthetic network infrastructure metrics, stores them in Apache Cassandra, and retrieves measurements through a command-line query tool.

## Overvie50 network resources:

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
| Total time-series records | **36,792,000** |
| Distinct resource-metric combinations | 350 |

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

The project has three main components:

| Component | Responsibility |
|---|---|
| `generate_dataset.py` | Generates reproducible synthetic measurements. |
| `ingest_data.py` | Loads measurements into Cassandra using concurrent prepared statements. |
| `query_data.py` | Retrieves measurements by resource, metric and time range, and exports the results to CSV. |

## Technology Stack

- Python
- Apache Cassandra 5.0.9
- Cassandra Python Driver 3.30.1
- Docker
- GitHub Actions

## Repository Structure

```text
network-monitoring-cassandra/
├── .github/
│   └── workflows/
│       └── python3-checks.yml
├── data/
│   └── sample_network_monitoring.csv
├── .gitignore
├── README.md
├── requirements.txt
├── generate_dataset.py
├── ingest_data.py
└── query_data.py
```

The full generated dataset is excluded from version control because of its size. A 1,000-record sample is provided to illustrate the CSV format.

## Getting Started

### Prerequisites

- Python 3.11 or later
- Docker
- Apache Cassandra 5.0.9
- Git

### 1. Clone the repository

```bash
git clone https://github.com/arman-h-alavi/network-monitoring-cassandra.git
cd network-monitoring-cassandra
```

### 2. Set up the Python environment

```bash
python3 -m venv cassandra-env
source cassandra-env/bin/activate
pip install -r requirements.txt
```

### 3. Start Cassandra

Run a single-node Cassandra container with a persistent Docker volume:

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
    PRIMARY KEY ((resource, bucket_start), metric_name, collected_at)
)
WITH CLUSTERING ORDER BY (metric_name ASC, collected_at ASC);
```

Exit `cqlsh`:

```sql
EXIT;
```

### 5. Generate the dataset

```bash
python3 generate_dataset.py
```

The script generates `network_monitoring.csv` in the current working directory.

The generator uses a fixed random seed (`42`) to make the output reproducible.

### 6. Ingest the data

```bash
python3 ingest_data.py
```

The ingestion script expects `network_monitoring.csv` in the current working directory. It connects to Cassandra at `127.0.0.1:9042`.

## Cassandra Data Model

The database uses two tables designed around their intended access patterns.

### `metrics_by_resource`

Stores the distinct metrics associated with each resource.

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

The time-series table uses seven-day buckets, anchored at November 1, 2025.

The ingestion script calculates the beginning of the relevant bucket using:

```python3
days_from_anchor = (
    collected_at.date() - BUCKET_ANCHOR
).days

bucket_start = BUCKET_ANCHOR + timedelta(
    days=(days_from_anchor // 7) * 7
)
```

Each complete bucket contains:

- 7 days
- 288 sampling intervals per day
- 7 metrics per interval
- **14,112 records per resource**

The one-year dataset produces 53 buckets per resource: 52 complete seven-day buckets and one final one-day bucket.

This design bounds partition size and supports time-range queries using the complete partition key.

## Querying Measurements

`query_data.py` provides a command-line interface for retrieving measurements.

It accepts:

- A resource ID
- One or more metric names
- Start and end timestamps
- An output CSV filename

Example:

```bash
python3 query_data.py \
  --resource RTR-001 \
  --metrics "CPU Usage,Memory Usage" \
  --start "2025-11-01 00:00" \
  --end "2025-11-03 00:00" \
  --output query_results.csv
```

The end timestamp is exclusive.

For ranges spanning multiple seven-day buckets, the script calculates the relevant bucket start dates and queries each partition separately. It uses prepared statements and the table's partition-key design rather than unrestricted filtering.

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

Run `python3 query_data.py --help` to see all available arguments.

## Data Simulation Assumptions

The generator uses resource-specific baseline profiles and introduces variation through:

- Daily workload cycles.
- Lower weekend activity.
- Resource-specific traffic, CPU and memory factors.
- Random variation in measurements.
- Workload-dependent packet drops and latency.

Measurement units:

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

## Design Decisions

- **Time-series partitioning:** Seven-day buckets bound the amount of data stored in each resource partition.
- **Prepared statements:** Reused CQL statements avoid repeatedly preparing identical queries.
- **Concurrent ingestion:** The Python driver executes bounded groups of concurrent inserts to improve ingestion throughput.
- **Query-oriented schema:** Partition and clustering keys reflect the intended resource, metric and time-range access patterns.
- **Single-node deployment:** Replication factor one and `SimpleStrategy` are used for local demonstration. This configuration does not provide production-grade fault tolerance or high availability.

## Continuous Integration

GitHub Actions runs on pushes and pull requests.

The workflow checks out the repository, sets up Python 3.11, installs the declared dependency, and compiles all three Python scripts to detect syntax errors.

This is a lightweight CI check; it does not perform database integration or ingestion-performance tests.

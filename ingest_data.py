
import csv
from datetime import datetime, date, timedelta

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent

# Configuration
CSV_FILE = "network_monitoring.csv"
CASSANDRA_HOST = "127.0.0.1"
CASSANDRA_PORT = 9042

CONCURRENCY = 32
CHUNK_SIZE = 500

# Seven-day buckets start from the dataset's first date.
BUCKET_ANCHOR = date(2025, 11, 1)

# Connect to Cassandra.
cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
session = cluster.connect("network_monitoring")

# Prepare statements once; reuse them for every insert.
time_series_insert = session.prepare("""
    INSERT INTO resource_metrics_by_period
    (resource, bucket_start, metric_name, collected_at, value)
    VALUES (?, ?, ?, ?, ?)
""")

metadata_insert = session.prepare("""
    INSERT INTO metrics_by_resource
    (resource, metric_name)
    VALUES (?, ?)
""")

def execute_batch(statements):
    """Execute a bounded group of concurrent statements."""
    if statements:
        execute_concurrent(
            session,
            statements,
            concurrency=CONCURRENCY,
            raise_on_first_error=True,
        )
        statements.clear()


try:
    # Read the CSV and ingest time-series measurements.
    statements = []
    records_processed = 0

    print("Starting time-series ingestion...")

    with open(CSV_FILE, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            resource = row["resource"]
            metric_name = row["metric_name"]
            collected_at = datetime.strptime(
                row["collected_at"], "%Y-%m-%d %H:%M"
            )
            value = float(row["value"])

            # Calculate the beginning of the seven-day bucket.
            days_from_anchor = (
                collected_at.date() - BUCKET_ANCHOR
            ).days

            bucket_start = BUCKET_ANCHOR + timedelta(
                days=(days_from_anchor // 7) * 7
            )

            parameters = (
                resource,
                bucket_start,
                metric_name,
                collected_at,
                value,
            )

            statements.append((time_series_insert, parameters))
            records_processed += 1

            # Keep the number of pending statements bounded.
            if len(statements) >= CHUNK_SIZE:
                execute_batch(statements)

            if records_processed % 100_000 == 0:
                print(
                    f"Processed {records_processed:,} records..."
                )

    # Execute the remaining statements.
    execute_batch(statements)

    print(
        f"Time-series ingestion completed: "
        f"{records_processed:,} records."
    )

    # Build the distinct resource-metric combinations.
    print("Starting metadata ingestion...")

    resource_metrics = set()

    with open(CSV_FILE, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            resource_metrics.add(
                (row["resource"], row["metric_name"])
            )

    metadata_statements = [
        (metadata_insert, pair)
        for pair in sorted(resource_metrics)
    ]

    execute_batch(metadata_statements)

    print(
        f"Metadata ingestion completed: "
        f"{len(resource_metrics)} combinations."
    )

    print("All ingestion operations completed successfully.")

finally:
    cluster.shutdown()

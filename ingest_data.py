
import csv
from datetime import date, datetime, timedelta

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


def get_bucket_start(collected_at, bucket_anchor=BUCKET_ANCHOR):
    """Calculate the beginning of the seven-day bucket."""
    days_from_anchor = (collected_at.date() - bucket_anchor).days

    return bucket_anchor + timedelta(
        days=(days_from_anchor // 7) * 7
    )


def prepare_statements(session):
    """Prepare the CQL statements used during ingestion."""
    time_series_insert = session.prepare(
        """
        INSERT INTO resource_metrics_by_period
        (resource, bucket_start, metric_name, collected_at, value)
        VALUES (?, ?, ?, ?, ?)
        """
    )

    metadata_insert = session.prepare(
        """
        INSERT INTO metrics_by_resource
        (resource, metric_name)
        VALUES (?, ?)
        """
    )

    return time_series_insert, metadata_insert


def execute_batch(
    session,
    statements,
    concurrency=CONCURRENCY,
):
    """Execute a bounded group of concurrent statements."""
    if statements:
        execute_concurrent(
            session,
            statements,
            concurrency=concurrency,
            raise_on_first_error=True,
        )
        statements.clear()


def ingest_time_series(
    session,
    time_series_insert,
    csv_file=CSV_FILE,
    chunk_size=CHUNK_SIZE,
    concurrency=CONCURRENCY,
):
    """Read the CSV and ingest time-series measurements."""
    statements = []
    records_processed = 0

    print("Starting time-series ingestion...")

    with open(csv_file, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            resource = row["resource"]
            metric_name = row["metric_name"]
            collected_at = datetime.strptime(
                row["collected_at"], "%Y-%m-%d %H:%M"
            )
            value = float(row["value"])

            bucket_start = get_bucket_start(collected_at)

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
            if len(statements) >= chunk_size:
                execute_batch(
                    session, statements, concurrency
                )

            if records_processed % 100_000 == 0:
                print(
                    f"Processed {records_processed:,} records..."
                )

    # Execute the remaining statements.
    execute_batch(session, statements, concurrency)

    print(
        f"Time-series ingestion completed: "
        f"{records_processed:,} records."
    )

    return records_processed


def ingest_metadata(
    session,
    metadata_insert,
    csv_file=CSV_FILE,
    concurrency=CONCURRENCY,
):
    """Ingest distinct resource-metric combinations."""
    print("Starting metadata ingestion...")

    resource_metrics = set()

    with open(csv_file, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            resource_metrics.add(
                (row["resource"], row["metric_name"])
            )

    metadata_statements = [
        (metadata_insert, pair)
        for pair in sorted(resource_metrics)
    ]

    execute_batch(
        session, metadata_statements, concurrency
    )

    print(
        f"Metadata ingestion completed: "
        f"{len(resource_metrics)} combinations."
    )

    return len(resource_metrics)


def main():
    """Connect to Cassandra and run both ingestion operations."""
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
    )

    try:
        session = cluster.connect("network_monitoring")

        time_series_insert, metadata_insert = prepare_statements(
            session
        )

        ingest_time_series(session, time_series_insert)
        ingest_metadata(session, metadata_insert)

        print("All ingestion operations completed successfully.")

    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()


import csv
import os
import uuid
from datetime import date, datetime

import pytest
from cassandra.cluster import Cluster

from ingest_data import ingest_metadata, ingest_time_series, prepare_statements
from query_data import query_measurements

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))

TEST_KEYSPACE = "network_monitoring_test"


@pytest.fixture(scope="module")
def cassandra_session():
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
    )

    setup_session = cluster.connect()

    setup_session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {TEST_KEYSPACE}
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }}
        """
    )

    session = cluster.connect(TEST_KEYSPACE)

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS metrics_by_resource (
            resource text,
            metric_name text,
            PRIMARY KEY (resource, metric_name)
        )
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_metrics_by_period (
            resource text,
            bucket_start date,
            metric_name text,
            collected_at timestamp,
            value double,
            PRIMARY KEY (
                (resource, bucket_start),
                metric_name,
                collected_at
            )
        ) WITH CLUSTERING ORDER BY (
            metric_name ASC,
            collected_at ASC
        )
        """
    )

    yield session

    cluster.shutdown()


def test_ingestion_and_query_round_trip(
    cassandra_session,
    tmp_path,
):
    resource = f"integration-test-{uuid.uuid4().hex}"

    csv_file = tmp_path / "integration_sample.csv"

    records = [
        {
            "resource": resource,
            "metric_name": "CPU Usage",
            "collected_at": "2025-11-01 00:00",
            "value": "45.5",
        },
        {
            "resource": resource,
            "metric_name": "Memory Usage",
            "collected_at": "2025-11-01 00:05",
            "value": "62.3",
        },
    ]

    try:
        with csv_file.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as file:
            writer = csv.DictWriter(
                file,
                fieldnames=[
                    "resource",
                    "metric_name",
                    "collected_at",
                    "value",
                ],
            )
            writer.writeheader()
            writer.writerows(records)

        time_series_insert, metadata_insert = prepare_statements(
            cassandra_session
        )

        ingest_time_series(
            cassandra_session,
            time_series_insert,
            csv_file=str(csv_file),
        )

        ingest_metadata(
            cassandra_session,
            metadata_insert,
            csv_file=str(csv_file),
        )

        results = query_measurements(
            cassandra_session,
            resource,
            ["CPU Usage", "Memory Usage"],
            datetime(2025, 11, 1, 0, 0),
            datetime(2025, 11, 1, 0, 10),
        )

        assert len(results) == 2

        actual = {
            (row[1], row[3])
            for row in results
        }

        assert actual == {
            ("CPU Usage", 45.5),
            ("Memory Usage", 62.3),
        }

        metadata = cassandra_session.execute(
            """
            SELECT metric_name
            FROM metrics_by_resource
            WHERE resource = %s
            """,
            (resource,),
        )

        assert {
            row.metric_name for row in metadata
        } == {"CPU Usage", "Memory Usage"}

    finally:
        cassandra_session.execute(
            """
            DELETE FROM resource_metrics_by_period
            WHERE resource = %s AND bucket_start = %s
            """,
            (resource, date(2025, 11, 1)),
        )

        cassandra_session.execute(
            """
            DELETE FROM metrics_by_resource
            WHERE resource = %s
            """,
            (resource,),
        )

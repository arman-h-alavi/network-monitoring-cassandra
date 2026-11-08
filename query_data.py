import argparse
import csv
from datetime import date, datetime, time, timedelta

from cassandra.cluster import Cluster


BUCKET_ANCHOR = date(2025, 11, 1)

VALID_METRICS = {
    "CPU Usage",
    "Memory Usage",
    "Incoming Traffic",
    "Outgoing Traffic",
    "Packets Received",
    "Packets Dropped",
    "Network Latency",
}


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Retrieve network monitoring measurements from Cassandra."
    )

    parser.add_argument(
        "--resource",
        required=True,
        help="Resource ID, e.g. RTR-001",
    )
    parser.add_argument(
        "--metrics",
        required=True,
        help='Comma-separated metric names, e.g. "CPU Usage,Memory Usage"',
    )
    parser.add_argument(
        "--start",
        required=True,
        help='Start timestamp in "YYYY-MM-DD HH:MM" format (inclusive)',
    )
    parser.add_argument(
        "--end",
        required=True,
        help='End timestamp in "YYYY-MM-DD HH:MM" format (exclusive)',
    )
    parser.add_argument(
        "--output",
        default="query_results.csv",
        help="Output CSV filename (default: query_results.csv)",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Cassandra host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9042,
        help="Cassandra port (default: 9042)",
    )

    return parser.parse_args()


def get_bucket_starts(start, end):
    """Return all seven-day bucket start dates overlapping the query range."""
    first_bucket = BUCKET_ANCHOR + timedelta(
        days=((start.date() - BUCKET_ANCHOR).days // 7) * 7
    )

    bucket_starts = []
    bucket_start = first_bucket

    while bucket_start < end.date() or bucket_start == end.date():
        bucket_starts.append(bucket_start)
        bucket_start += timedelta(days=7)

    return bucket_starts


def main():
    args = parse_arguments()

    try:
        start = datetime.strptime(args.start, "%Y-%m-%d %H:%M")
        end = datetime.strptime(args.end, "%Y-%m-%d %H:%M")
    except ValueError:
        raise SystemExit(
            'Error: timestamps must use the format "YYYY-MM-DD HH:MM".'
        )

    if start < datetime.combine(BUCKET_ANCHOR, time.min):
        raise SystemExit("Error: the start timestamp precedes the dataset.")

    if end <= start:
        raise SystemExit("Error: the end timestamp must be later than the start.")

    metrics = [metric.strip() for metric in args.metrics.split(",")]

    if not metrics or any(not metric for metric in metrics):
        raise SystemExit("Error: provide at least one valid metric name.")

    invalid_metrics = set(metrics) - VALID_METRICS

    if invalid_metrics:
        raise SystemExit(
            f"Error: unsupported metric name(s): {', '.join(sorted(invalid_metrics))}"
        )

    if len(metrics) != len(set(metrics)):
        raise SystemExit("Error: duplicate metric names were provided.")

    bucket_starts = get_bucket_starts(start, end)
    results = []

    cluster = Cluster([args.host], port=args.port)

    try:
        session = cluster.connect("network_monitoring")

        query = session.prepare("""
            SELECT collected_at, value
            FROM resource_metrics_by_period
            WHERE resource = ?
              AND bucket_start = ?
              AND metric_name = ?
              AND collected_at >= ?
              AND collected_at < ?
        """)

        print(f"Querying resource: {args.resource}")
        print(f"Metrics: {', '.join(metrics)}")
        print(f"Time range: {args.start} to {args.end} (end exclusive)")
        print(f"Partitions to inspect: {len(bucket_starts) * len(metrics)}")

        for bucket_start in bucket_starts:
            bucket_end = bucket_start + timedelta(days=7)

            range_start = max(
                start,
                datetime.combine(bucket_start, time.min),
            )
            range_end = min(
                end,
                datetime.combine(bucket_end, time.min),
            )

            if range_start >= range_end:
                continue

            for metric in metrics:
                rows = session.execute(
                    query,
                    (
                        args.resource,
                        bucket_start,
                        metric,
                        range_start,
                        range_end,
                    ),
                )

                for row in rows:
                    results.append(
                        (
                            args.resource,
                            metric,
                            row.collected_at,
                            row.value,
                        )
                    )

    finally:
        cluster.shutdown()

    results.sort(key=lambda row: (row[1], row[2]))

    with open(args.output, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            ["resource", "metric_name", "collected_at", "value"]
        )
        writer.writerows(results)

    print(f"Retrieved {len(results):,} measurements.")
    print(f"Results saved to: {args.output}")


if __name__ == "__main__":
    main()

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


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(
        description="Retrieve network monitoring measurements from Cassandra."
    )
    parser.add_argument(
        "--resource", required=True, help="Resource ID, e.g. RTR-001"
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
        "--host", default="127.0.0.1", help="Cassandra host (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", type=int, default=9042, help="Cassandra port (default: 9042)"
    )

    return parser.parse_args(argv)


def parse_timestamps(start_text, end_text):
    try:
        start = datetime.strptime(start_text, "%Y-%m-%d %H:%M")
        end = datetime.strptime(end_text, "%Y-%m-%d %H:%M")
    except ValueError:
        raise SystemExit(
            'Error: timestamps must use the format "YYYY-MM-DD HH:MM".'
        )

    if start < datetime.combine(BUCKET_ANCHOR, time.min):
        raise SystemExit("Error: the start timestamp precedes the dataset.")

    if end <= start:
        raise SystemExit("Error: the end timestamp must be later than the start.")

    return start, end


def parse_metrics(metrics_text):
    metrics = [metric.strip() for metric in metrics_text.split(",")]

    if not metrics or any(not metric for metric in metrics):
        raise SystemExit("Error: provide at least one valid metric name.")

    invalid_metrics = set(metrics) - VALID_METRICS

    if invalid_metrics:
        raise SystemExit(
            "Error: unsupported metric name(s): "
            f"{', '.join(sorted(invalid_metrics))}"
        )

    if len(metrics) != len(set(metrics)):
        raise SystemExit("Error: duplicate metric names were provided.")

    return metrics


def get_bucket_starts(start, end):
    """Return seven-day bucket starts overlapping the half-open query range."""
    first_bucket = BUCKET_ANCHOR + timedelta(
        days=((start.date() - BUCKET_ANCHOR).days // 7) * 7
    )

    bucket_starts = []
    bucket_start = first_bucket

    while datetime.combine(bucket_start, time.min) < end:
        bucket_starts.append(bucket_start)
        bucket_start += timedelta(days=7)

    return bucket_starts


def query_measurements(session, resource, metrics, start, end):
    bucket_starts = get_bucket_starts(start, end)
    results = []

    query = session.prepare(
        """
        SELECT collected_at, value
        FROM resource_metrics_by_period
        WHERE resource = ?
          AND bucket_start = ?
          AND metric_name = ?
          AND collected_at >= ?
          AND collected_at < ?
        """
    )

    for bucket_start in bucket_starts:
        bucket_end = bucket_start + timedelta(days=7)

        range_start = max(start, datetime.combine(bucket_start, time.min))
        range_end = min(end, datetime.combine(bucket_end, time.min))

        if range_start >= range_end:
            continue

        for metric in metrics:
            rows = session.execute(
                query,
                (
                    resource,
                    bucket_start,
                    metric,
                    range_start,
                    range_end,
                ),
            )

            for row in rows:
                results.append(
                    (resource, metric, row.collected_at, row.value)
                )

    results.sort(key=lambda row: (row[1], row[2]))

    return results


def write_results(results, output_file):
    with open(output_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["resource", "metric_name", "collected_at", "value"])
        writer.writerows(results)


def main():
    args = parse_arguments()

    start, end = parse_timestamps(args.start, args.end)
    metrics = parse_metrics(args.metrics)

    print(f"Querying resource: {args.resource}")
    print(f"Time range: {start} to {end} (end exclusive)")
    print(f"Metrics: {', '.join(metrics)}")

    cluster = Cluster([args.host], port=args.port)

    try:
        session = cluster.connect("network_monitoring")

        results = query_measurements(
            session,
            args.resource,
            metrics,
            start,
            end,
        )
    finally:
        cluster.shutdown()

    write_results(results, args.output)

    print(f"Retrieved {len(results):,} records.")
    print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()

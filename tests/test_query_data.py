
from datetime import datetime

import pytest

from query_data import (
    get_bucket_starts,
    parse_arguments,
    parse_metrics,
    parse_timestamps,
)


def test_parse_timestamps_valid():
    start, end = parse_timestamps(
        "2025-11-01 10:00",
        "2025-11-01 11:00",
    )

    assert start == datetime(2025, 11, 1, 10, 0)
    assert end == datetime(2025, 11, 1, 11, 0)


def test_parse_timestamps_invalid_format():
    with pytest.raises(SystemExit):
        parse_timestamps("2025/11/01 10:00", "2025-11-01 11:00")


def test_parse_timestamps_before_dataset():
    with pytest.raises(SystemExit):
        parse_timestamps("2025-10-31 23:55", "2025-11-01 01:00")


def test_parse_timestamps_end_not_after_start():
    with pytest.raises(SystemExit):
        parse_timestamps("2025-11-01 11:00", "2025-11-01 10:00")


def test_parse_metrics_valid():
    metrics = parse_metrics("CPU Usage, Memory Usage")

    assert metrics == ["CPU Usage", "Memory Usage"]


def test_parse_metrics_removes_whitespace():
    metrics = parse_metrics(" CPU Usage , Memory Usage ")

    assert metrics == ["CPU Usage", "Memory Usage"]


def test_parse_metrics_rejects_unsupported_metric():
    with pytest.raises(SystemExit):
        parse_metrics("CPU Usage, Temperature")


def test_parse_metrics_rejects_empty_metric():
    with pytest.raises(SystemExit):
        parse_metrics("CPU Usage, , Memory Usage")


def test_parse_metrics_rejects_duplicates():
    with pytest.raises(SystemExit):
        parse_metrics("CPU Usage, Memory Usage, CPU Usage")


def test_parse_arguments():
    args = parse_arguments(
        [
            "--resource",
            "RTR-001",
            "--metrics",
            "CPU Usage",
            "--start",
            "2025-11-01 10:00",
            "--end",
            "2025-11-01 11:00",
        ]
    )

    assert args.resource == "RTR-001"
    assert args.metrics == "CPU Usage"
    assert args.start == "2025-11-01 10:00"
    assert args.end == "2025-11-01 11:00"
    assert args.output == "query_results.csv"
    assert args.host == "127.0.0.1"
    assert args.port == 9042


def test_get_bucket_starts_within_one_bucket():
    start = datetime(2025, 11, 2, 10, 0)
    end = datetime(2025, 11, 5, 10, 0)

    assert get_bucket_starts(start, end) == [
        datetime(2025, 11, 1).date()
    ]


def test_get_bucket_starts_across_boundary():
    start = datetime(2025, 11, 7, 23, 0)
    end = datetime(2025, 11, 8, 1, 0)

    assert get_bucket_starts(start, end) == [
        datetime(2025, 11, 1).date(),
        datetime(2025, 11, 8).date(),
    ]


def test_get_bucket_starts_excludes_bucket_at_exclusive_end():
    start = datetime(2025, 11, 1, 0, 0)
    end = datetime(2025, 11, 8, 0, 0)

    assert get_bucket_starts(start, end) == [
        datetime(2025, 11, 1).date()
    ]


def test_get_bucket_starts_across_multiple_buckets():
    start = datetime(2025, 11, 2, 0, 0)
    end = datetime(2025, 11, 16, 0, 0)

    assert get_bucket_starts(start, end) == [
        datetime(2025, 11, 1).date(),
        datetime(2025, 11, 8).date(),
        datetime(2025, 11, 15).date(),
    ]

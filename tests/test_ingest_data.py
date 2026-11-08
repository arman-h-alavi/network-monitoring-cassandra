
import csv
from datetime import date, datetime, timedelta

import ingest_data


def test_get_bucket_start():
    test_cases = [
        (datetime(2025, 11, 1), date(2025, 11, 1)),
        (datetime(2025, 11, 7, 23, 55), date(2025, 11, 1)),
        (datetime(2025, 11, 8), date(2025, 11, 8)),
        (datetime(2025, 11, 14, 23, 55), date(2025, 11, 8)),
        (datetime(2025, 11, 15), date(2025, 11, 15)),
    ]

    for collected_at, expected in test_cases:
        assert ingest_data.get_bucket_start(collected_at) == expected


def test_get_bucket_start_uses_seven_day_intervals():
    anchor = ingest_data.BUCKET_ANCHOR

    for day_offset in range(365):
        collected_at = datetime.combine(
            anchor + timedelta(days=day_offset),
            datetime.min.time(),
        )

        expected = anchor + timedelta(days=(day_offset // 7) * 7)

        assert ingest_data.get_bucket_start(collected_at) == expected


def test_execute_batch_calls_execute_concurrent(monkeypatch):
    statements = [
        ("statement_1", ("value_1",)),
        ("statement_2", ("value_2",)),
    ]

    captured = {}

    def fake_execute_concurrent(session, submitted, **kwargs):
        captured["session"] = session
        captured["statements"] = list(submitted)
        captured["kwargs"] = kwargs

    monkeypatch.setattr(
        ingest_data,
        "execute_concurrent",
        fake_execute_concurrent,
    )

    session = object()

    ingest_data.execute_batch(
        session,
        statements,
        concurrency=16,
    )

    assert captured["session"] is session
    assert captured["statements"] == [
        ("statement_1", ("value_1",)),
        ("statement_2", ("value_2",)),
    ]
    assert captured["kwargs"] == {
        "concurrency": 16,
        "raise_on_first_error": True,
    }

    # The original function clears the submitted list after execution.
    assert statements == []


def test_execute_batch_skips_empty_list(monkeypatch):
    def unexpected_call(*args, **kwargs):
        raise AssertionError("execute_concurrent should not be called")

    monkeypatch.setattr(
        ingest_data,
        "execute_concurrent",
        unexpected_call,
    )

    ingest_data.execute_batch(object(), [])


def test_ingest_time_series(tmp_path, monkeypatch):
    csv_file = tmp_path / "small_dataset.csv"

    rows = [
        {
            "resource": "RTR-001",
            "metric_name": "CPU Usage",
            "collected_at": "2025-11-01 00:00",
            "value": "25.5",
        },
        {
            "resource": "RTR-001",
            "metric_name": "Memory Usage",
            "collected_at": "2025-11-08 00:00",
            "value": "45.0",
        },
    ]

    with csv_file.open("w", newline="", encoding="utf-8") as file:
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
        writer.writerows(rows)

    captured = []

    def fake_execute_concurrent(session, statements, **kwargs):
        captured.extend(statements)

    monkeypatch.setattr(
        ingest_data,
        "execute_concurrent",
        fake_execute_concurrent,
    )

    session = object()
    prepared_statement = object()

    count = ingest_data.ingest_time_series(
        session,
        prepared_statement,
        csv_file=csv_file,
        chunk_size=10,
        concurrency=4,
    )

    assert count == 2
    assert len(captured) == 2

    assert captured[0] == (
        prepared_statement,
        (
            "RTR-001",
            date(2025, 11, 1),
            "CPU Usage",
            datetime(2025, 11, 1, 0, 0),
            25.5,
        ),
    )

    assert captured[1] == (
        prepared_statement,
        (
            "RTR-001",
            date(2025, 11, 8),
            "Memory Usage",
            datetime(2025, 11, 8, 0, 0),
            45.0,
        ),
    )


def test_ingest_metadata_deduplicates_resource_metrics(tmp_path, monkeypatch):
    csv_file = tmp_path / "metadata_dataset.csv"

    rows = [
        ("RTR-001", "CPU Usage"),
        ("RTR-001", "CPU Usage"),
        ("RTR-001", "Memory Usage"),
        ("FW-001", "CPU Usage"),
    ]

    with csv_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["resource", "metric_name"])
        writer.writerows(rows)

    captured = []

    def fake_execute_concurrent(session, statements, **kwargs):
        captured.extend(statements)

    monkeypatch.setattr(
        ingest_data,
        "execute_concurrent",
        fake_execute_concurrent,
    )

    session = object()
    prepared_statement = object()

    count = ingest_data.ingest_metadata(
        session,
        prepared_statement,
        csv_file=csv_file,
        concurrency=4,
    )

    assert count == 3

    assert captured == [
        (prepared_statement, ("FW-001", "CPU Usage")),
        (prepared_statement, ("RTR-001", "CPU Usage")),
        (prepared_statement, ("RTR-001", "Memory Usage")),
    ]


import csv
import math
from collections import Counter

from generate_dataset import METRICS, build_resources, generate_dataset


def test_build_resources():
    resources = build_resources()

    assert len(resources) == 50

    resource_types = Counter(resource_type for _, resource_type in resources)

    assert resource_types == {
        "router": 20,
        "firewall": 15,
        "switch": 15,
    }


def test_generate_dataset_record_count_and_structure(tmp_path):
    output_file = tmp_path / "test_dataset.csv"

    generate_dataset(
        output_file=output_file,
        days=1,
        interval_minutes=720,
    )

    with output_file.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    assert reader.fieldnames == [
        "resource",
        "metric_name",
        "collected_at",
        "value",
    ]

    # 50 resources × 7 metrics × 2 samples per day = 700 records.
    assert len(rows) == 700

    assert {row["resource"] for row in rows} == {
        resource for resource, _ in build_resources()
    }

    assert {row["metric_name"] for row in rows} == set(METRICS)



def test_generate_dataset_values_are_numeric_and_finite(tmp_path):
    output_file = tmp_path / "test_values.csv"

    generate_dataset(
        output_file=output_file,
        days=1,
        interval_minutes=720,
    )

    with output_file.open(newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))

    for row in rows:
        value = float(row["value"])
        assert math.isfinite(value)


def test_generate_dataset_is_reproducible(tmp_path):
    first_file = tmp_path / "first.csv"
    second_file = tmp_path / "second.csv"

    generate_dataset(
        output_file=first_file,
        days=1,
        interval_minutes=720,
        seed=42,
    )

    generate_dataset(
        output_file=second_file,
        days=1,
        interval_minutes=720,
        seed=42,
    )

    assert first_file.read_bytes() == second_file.read_bytes()

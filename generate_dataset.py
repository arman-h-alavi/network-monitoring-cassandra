import csv
import math
import random
from datetime import datetime, timedelta

# Reproducibility: the same seed produces the same dataset.
rng = random.Random(42)

# Dataset configuration
START_DATE = datetime(2025, 11, 1, 0, 0)
DAYS = 365
INTERVAL_MINUTES = 5
OUTPUT_FILE = "network_monitoring.csv"

# Resource definitions: (resource ID, type)
resources = []

for i in range(1, 21):
    resources.append((f"RTR-{i:03d}", "router"))

for i in range(1, 16):
    resources.append((f"FW-{i:03d}", "firewall"))

for i in range(1, 16):
    resources.append((f"SW-{i:03d}", "switch"))

# Seven metrics, shared across all resources.
METRICS = [
    "CPU Usage",
    "Memory Usage",
    "Incoming Traffic",
    "Outgoing Traffic",
    "Packets Received",
    "Packets Dropped",
    "Network Latency",
]

# Different baseline characteristics for each resource type.
PROFILES = {
    "router": {
        "cpu": 25, "memory": 45, "traffic": 120,
        "latency": 8, "drop_rate": 0.0005,
    },
    "firewall": {
        "cpu": 35, "memory": 55, "traffic": 80,
        "latency": 12, "drop_rate": 0.002,
    },
    "switch": {
        "cpu": 20, "memory": 40, "traffic": 180,
        "latency": 3, "drop_rate": 0.0003,
    },
}

total_records = len(resources) * len(METRICS) * DAYS * 288

print(f"Generating {total_records:,} records...")

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    # CSV header
    writer.writerow(["resource", "metric_name", "collected_at", "value"])

    for resource, resource_type in resources:
        profile = PROFILES[resource_type]

        # Small resource-specific differences
        traffic_factor = rng.uniform(0.8, 1.2)
        cpu_factor = rng.uniform(0.85, 1.15)
        memory_factor = rng.uniform(0.9, 1.1)

        for step in range(DAYS * 288):
            timestamp = START_DATE + timedelta(
                minutes=step * INTERVAL_MINUTES
            )

            hour = timestamp.hour + timestamp.minute / 60

            # Daily workload cycle: higher traffic during daytime.
            daily_cycle = 0.55 + 0.45 * (
                0.5 + 0.5 * math.sin((hour - 9) * math.pi / 12)
            )

            # Slightly lower activity on weekends.
            weekday_factor = 0.85 if timestamp.weekday() >= 5 else 1.0

            workload = daily_cycle * weekday_factor

            # Traffic measurements (Mbps)
            incoming = max(
                1,
                profile["traffic"]
                * traffic_factor
                * workload
                * rng.uniform(0.85, 1.15),
            )

            outgoing = max(
                1,
                incoming * rng.uniform(0.65, 0.95),
            )

            # CPU utilization (%)
            cpu = (
                profile["cpu"]
                * cpu_factor
                + 30 * workload
                + rng.gauss(0, 4)
            )
            cpu = round(max(1, min(100, cpu)), 2)

            # Memory utilization (%)
            memory = (
                profile["memory"]
                * memory_factor
                + 2 * math.sin(step / 500)
                + rng.gauss(0, 1.2)
            )
            memory = round(max(5, min(98, memory)), 2)

            # Estimated packets received in five minutes.
            packets_received = int(
                incoming * 1_000_000 * 300 / (8 * 1000)
            )

            # Packet loss increases slightly under heavier workload.
            drop_rate = profile["drop_rate"] * (
                1 + max(0, workload - 0.7)
            )

            packets_dropped = int(
                packets_received
                * drop_rate
                * rng.uniform(0.5, 1.5)
            )

            # Latency increases with workload.
            latency = (
                profile["latency"]
                + 8 * workload
                + rng.gauss(0, 1.5)
            )
            latency = round(max(0.5, latency), 2)

            measurements = [
                ("CPU Usage", cpu),
                ("Memory Usage", memory),
                ("Incoming Traffic", round(incoming, 2)),
                ("Outgoing Traffic", round(outgoing, 2)),
                ("Packets Received", packets_received),
                ("Packets Dropped", packets_dropped),
                ("Network Latency", latency),
            ]

            collected_at = timestamp.strftime("%Y-%m-%d %H:%M")

            for metric_name, value in measurements:
                writer.writerow([
                    resource,
                    metric_name,
                    collected_at,
                    value,
                ])

print(f"Finished. Created: {OUTPUT_FILE}")
print(f"Expected records: {total_records:,}")

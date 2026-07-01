import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

output_path = Path("raw/dataset.csv")
output_path.parent.mkdir(parents=True, exist_ok=True)

num_records = 1_000_000
categories = ["A", "B", "C", "D", "E"]
regions = ["Cairo", "Alexandria", "Giza", "Ismailia", "Mansoura"]
devices = ["mobile", "desktop", "tablet"]

start_time = datetime(2026, 1, 1)

with output_path.open("w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow([
        "category",
        "amount",
        "quantity",
        "region",
        "device",
        "user_id",
        "event_time",
        "status"
    ])

    for i in range(num_records):
        writer.writerow([
            random.choice(categories),
            round(random.uniform(10, 1000), 2),
            random.randint(1, 20),
            random.choice(regions),
            random.choice(devices),
            random.randint(1, 100000),
            (start_time + timedelta(seconds=i)).isoformat(),
            random.choice(["completed", "pending", "failed"])
        ])

print(f"Generated {num_records} records at {output_path}")

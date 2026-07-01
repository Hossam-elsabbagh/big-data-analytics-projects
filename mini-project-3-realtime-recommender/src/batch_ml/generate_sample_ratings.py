import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

output_path = Path("data/raw/ratings.csv")
output_path.parent.mkdir(parents=True, exist_ok=True)

num_records = 500_000
num_users = 5000
num_items = 1000

start_time = datetime(2026, 1, 1)

with output_path.open("w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["user_id", "item_id", "rating", "timestamp"])

    for i in range(num_records):
        user_id = random.randint(1, num_users)
        item_id = random.randint(1, num_items)
        rating = round(random.uniform(1.0, 5.0), 1)
        timestamp = start_time + timedelta(seconds=i)
        writer.writerow([user_id, item_id, rating, timestamp.isoformat()])

print(f"Generated {num_records} records at {output_path}")

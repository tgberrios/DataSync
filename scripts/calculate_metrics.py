import json
import random

metrics = []

for day in range(1, 8):
    metric = {
        "date": f"2024-01-{day:02d}",
        "total_users": random.randint(100, 1000),
        "active_sessions": random.randint(50, 500),
        "revenue": round(random.uniform(1000.0, 10000.0), 2),
        "conversion_rate": round(random.uniform(1.0, 10.0), 2)
    }
    metrics.append(metric)

print(json.dumps(metrics))


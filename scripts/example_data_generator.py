import json
import random
from datetime import datetime, timedelta

data = []

for i in range(1, 11):
    record = {
        "id": i,
        "name": f"Item {i}",
        "value": random.randint(10, 1000),
        "status": random.choice(["active", "inactive", "pending"]),
        "created_at": (datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S"),
        "score": round(random.uniform(0.0, 100.0), 2)
    }
    data.append(record)

print(json.dumps(data, indent=2))


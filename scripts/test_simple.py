import json

data = [
    {"id": 1, "name": "Test 1", "value": 100},
    {"id": 2, "name": "Test 2", "value": 200},
    {"id": 3, "name": "Test 3", "value": 300}
]

print(json.dumps(data))


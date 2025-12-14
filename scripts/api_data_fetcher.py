import json
import urllib.request
import urllib.error

try:
    url = "https://jsonplaceholder.typicode.com/users"
    with urllib.request.urlopen(url) as response:
        users = json.loads(response.read().decode())
    
    data = []
    for user in users[:5]:
        record = {
            "user_id": user["id"],
            "name": user["name"],
            "email": user["email"],
            "city": user["address"]["city"],
            "company": user["company"]["name"]
        }
        data.append(record)
    
    print(json.dumps(data))
except Exception as e:
    error_data = [{"error": str(e)}]
    print(json.dumps(error_data))


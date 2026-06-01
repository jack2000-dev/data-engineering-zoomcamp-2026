import requests
import dlt
from dlt.sources.rest_api import rest_api_resources

print("Testing direct requests...")
url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
try:
    resp = requests.get(url + "?page=1")
    data = resp.json()
    print(f"Direct request length for page=1: {len(data)}")
    if isinstance(data, list) and len(data) > 0:
        print("First item preview:", data[0])
except Exception as e:
    print(f"Request failed: {e}")

print("\nTesting dlt extraction explicitly...")
@dlt.source
def taxi_source():
    config = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resources": [
            {
                "name": "taxi_data",
                "endpoint": {
                    "path": "",
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 1
                    }
                }
            }
        ]
    }
    yield from rest_api_resources(config)

for item in taxi_source().resources["taxi_data"]:
    print(f"Extracted a batch! Got {len(item) if isinstance(item, list) else 1} items.")
    break

import dlt
from dlt.sources.rest_api import rest_api_resources

@dlt.source
def taxi_source():
    """Define the NYC Taxi data REST API source."""
    config = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net",
        },
        "resources": [
            {
                "name": "taxi_data",
                "endpoint": {
                    "path": "data_engineering_zoomcamp_api",
                    # The "page_number" paginator automatically uses `page=1`, `page=2`, etc.
                    # and will automatically stop when the API returns an empty list `[]`.
                    "paginator": "page_number",
                    # The API returns a JSON array at the root. `$[*]` tells dlt to extract each item in the array.
                    "data_selector": "$[*]"
                }
            }
        ]
    }
    
    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="taxi_dataset",
    progress="log"
)

if __name__ == "__main__":
    load_info = pipeline.run(taxi_source())
    print(load_info)

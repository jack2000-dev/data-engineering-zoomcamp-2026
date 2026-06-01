"""Pipeline to ingest book data from the Open Library Search API using dlt REST API source."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_source():
    """Define dlt resources from Open Library REST API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org",
            "headers": {
                "User-Agent": "dlt-workshop/1.0 (data-engineering-zoomcamp)",
            },
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "search.json",
                    "data_selector": "docs",
                    "params": {
                        "q": "data engineering",
                        "fields": "key,title,author_name,first_publish_year,number_of_pages_median,ratings_average,ratings_count",
                        "limit": 50,
                    },
                    "paginator": "single_page",
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="open_library_pipeline",
    destination="duckdb",
    dataset_name="open_library_data",
    # drop and reload on each run (remove once pipeline is stable)
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_source())
    print(load_info)  # noqa: T201

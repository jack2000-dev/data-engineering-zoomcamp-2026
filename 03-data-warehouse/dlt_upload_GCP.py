import base64
import json
import os
from io import BytesIO
from pathlib import Path

import dlt
import pandas as pd
import requests
from dotenv import load_dotenv
from dlt.destinations import filesystem


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

creds = json.loads(
    base64.b64decode(os.environ["SECRET_GCP_CREDS"]).decode("utf-8")
)

BUCKET_URL = "gs://your_bucket_name"


@dlt.source(name="rides")
def download_parquet():
    prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata"

    for month in range(1, 7):
        url = f"{prefix}_2024-{month:02d}.parquet"
        response = requests.get(url)
        response.raise_for_status()

        df = pd.read_parquet(BytesIO(response.content))
        yield dlt.resource(df, name=f"yellow_tripdata_2024_{month:02d}")


pipeline = dlt.pipeline(
    pipeline_name="rides_pipeline",
    destination=filesystem(
        bucket_url=BUCKET_URL,
        credentials=creds,
        layout="{schema_name}/{table_name}.{ext}",
    ),
    dataset_name="rides_dataset",
)

load_info = pipeline.run(download_parquet(), loader_file_format="parquet")
print(load_info)

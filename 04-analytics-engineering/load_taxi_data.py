import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import storage


# Change these if needed, or override them with environment variables.
BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "de_zoomcamp_2026_demo")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "04-analytics-engineering/raw")
DOWNLOAD_DIR = Path(os.environ.get("DOWNLOAD_DIR", "data"))

# Homework 4 needs the static DataTalksClub files, not the latest TLC files.
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

# Load yellow and green first for the main dbt models.
# Later, use TAXI_TYPES=fhv if you want to load FHV for homework question 6.
TAXI_TYPES = os.environ.get("TAXI_TYPES", "yellow,green").split(",")
MONTHS = [f"{i:02d}" for i in range(1, 13)]
YEARS_BY_TAXI_TYPE = {
    "yellow": ["2019", "2020"],
    "green": ["2019", "2020"],
    "fhv": ["2019"],
}

CHUNK_SIZE = 8 * 1024 * 1024

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)


def create_bucket(bucket_name):
    try:
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]

        if bucket_name in project_bucket_ids:
            print(f"Bucket '{bucket_name}' exists and belongs to your project.")
            return

        # If get_bucket works but list_buckets did not include it, the name is
        # probably taken by another project/account.
        client.get_bucket(bucket_name)
        print(
            f"A bucket with the name '{bucket_name}' already exists, "
            "but it does not belong to your project."
        )
        sys.exit(1)

    except NotFound:
        client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        print(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. "
            "Please use a different bucket name."
        )
        sys.exit(1)


def build_file_specs():
    file_specs = []

    for taxi_type in TAXI_TYPES:
        taxi_type = taxi_type.strip()
        years = YEARS_BY_TAXI_TYPE.get(taxi_type)

        if years is None:
            valid_types = ", ".join(YEARS_BY_TAXI_TYPE)
            raise ValueError(f"Unknown taxi type '{taxi_type}'. Use one of: {valid_types}")

        for year in years:
            for month in MONTHS:
                file_name = f"{taxi_type}_tripdata_{year}-{month}.csv.gz"
                url = f"{BASE_URL}/{taxi_type}/{file_name}"
                local_path = DOWNLOAD_DIR / taxi_type / file_name
                blob_name = f"{GCS_PREFIX}/{taxi_type}/{file_name}"
                file_specs.append((url, local_path, blob_name))

    return file_specs


def download_file(file_spec):
    url, local_path, blob_name = file_spec
    local_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, local_path)
        print(f"Downloaded: {local_path}")
        return local_path, blob_name
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_info, max_retries=3):
    if file_info is None:
        return

    file_path, blob_name = file_info
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    if verify_gcs_upload(blob_name):
        print(f"Already uploaded: gs://{BUCKET_NAME}/{blob_name}")
        return

    for attempt in range(max_retries):
        try:
            print(
                f"Uploading {file_path} to gs://{BUCKET_NAME}/{blob_name} "
                f"(Attempt {attempt + 1})..."
            )
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return

            print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    create_bucket(BUCKET_NAME)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    file_specs = build_file_specs()

    with ThreadPoolExecutor(max_workers=4) as executor:
        downloaded_files = list(executor.map(download_file, file_specs))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, downloaded_files)

    print("All files processed and verified.")

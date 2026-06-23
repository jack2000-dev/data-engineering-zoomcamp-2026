import dataclasses
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer
from models import Ride, ride_from_now


url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount",
]
df = pd.read_parquet(url, columns=columns)


def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode("utf-8")


# Connect to Kafka
server = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=[server],  # for container -> redpanda:29092
    value_serializer=ride_serializer,
)
t0 = time.time()

topic_name = "green-trips"

# _ is Python's standard "throwaway variable"
for _, row in df.iterrows():
    ride = ride_from_now(row)
    producer.send(topic_name, value=ride)
    # print(f"Sent: {ride}")
    # time.sleep(0.01)

producer.flush()

t1 = time.time()
print(f"took {(t1 - t0):.2f} seconds")

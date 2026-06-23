import json
from dataclasses import dataclass
import pandas as pd

@dataclass
class Ride:
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    passenger_count: float
    trip_distance: float
    tip_amount: float
    total_amount: float

def ride_from_now(row):
    # Convert timestamp to str
    pickup_dt_str = row['lpep_pickup_datetime'].strftime('%Y-%m-%d %H:%M:%S')
    dropoff_dt_str = row['lpep_dropoff_datetime'].strftime('%Y-%m-%d %H:%M:%S')

    # Resolve NaN in passenger_count
    passenger_count_cnvrt = float(row['passenger_count']) if pd.notna(row['passenger_count']) else None
    
    return Ride(
lpep_pickup_datetime=pickup_dt_str,
    lpep_dropoff_datetime=dropoff_dt_str,
      PULocationID=int(row['PULocationID']),
      DOLocationID=int(row['DOLocationID']),
      passenger_count=passenger_count_cnvrt,
      trip_distance=float(row['trip_distance']),
      tip_amount=float(row['tip_amount']),
      total_amount=float(row['total_amount']),
    )

def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)
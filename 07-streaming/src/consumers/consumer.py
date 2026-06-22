# Consumer: reads messages from Kafka (the opposite of the producer)
import sys
from datetime import datetime
from pathlib import Path

# Add parent folder to Python's search path so `from models import ...` works
sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import ride_deserializer  # turns bytes back into a Ride object

# Kafka broker address: localhost:9092 = external (for your laptop)
server = "localhost:9092"
topic_name = "rides"  # the Kafka "channel" the producer sent to

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],  # where to connect
    auto_offset_reset="earliest",  # new group reads from message #0 (not just new ones)
    group_id="rides-console",  # group name = Kafka's bookmark; same group = resume, new group = restart
    value_deserializer=ride_deserializer,  # bytes → Ride, applied to every message
)

print(f"Listening to {topic_name}...")

# The consumer loop pulls messages one at a time. It runs forever by default;
# we stop after 10 for this demo.
count = 0
for message in consumer:
    ride = message.value  # already a Ride (deserialized above)
    # Convert epoch milliseconds back to a readable datetime (undo producer's * 1000)
    pickup_dt = datetime.fromtimestamp(ride.tpep_pickup_datetime / 1000)
    print(
        f"Received: PU={ride.PULocationID}, DO={ride.DOLocationID}, "
        f"distance={ride.trip_distance}, amount=${ride.total_amount:.2f}, "
        f"pickup={pickup_dt}"
    )
    count += 1
    if count >= 10:  # escape hatch for demoing
        print(
            f"\n... received {count} messages so far (stopping after 10 for demo purposes)"
        )
        break  # exit the loop

# Close the connection: saves our reading position (offset) and leaves the group
consumer.close()

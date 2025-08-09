import time
import redis
import json
from config import R

STREAM_KEY = "raw_sensor_data"
GROUP_NAME = "detection_group"
CONSUMER_NAME = "detector-1"
ANOMALY_THRESHOLD = 115.0

def setup_stream_group():
    """Create the stream consumer group. Ignore error if it already exists."""
    try:
        R.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
        print("Created consumer group.")
    except redis.exceptions.ResponseError:
        print("Consumer group already exists.")

def process_stream():
    """Read from stream, detect anomalies, and update Redis."""
    setup_stream_group()
    while True:
        # Read from the stream using the consumer group
        messages = R.xreadgroup(GROUP_NAME, CONSUMER_NAME, {STREAM_KEY: ">"}, count=1, block=2000)

        if not messages:
            continue

        for stream, entries in messages:
            for entry_id, data in entries:
                sensor_id = data['sensor_id']
                value = float(data['value'])
                ts = int(time.time() * 1000)

                # Determine new status
                status = "ANOMALY" if value > ANOMALY_THRESHOLD else "NORMAL"
                
                # Get the full sensor data
                sensor_info = R.json().get(f"sensor:{sensor_id}")
                sensor_info.update({"last_value": value, "status": status, "timestamp": ts})
                
                # Update Current State
                R.hset(f"sensor_state:{sensor_id}", mapping={
                    "last_value": value,
                    "status": status,
                    "timestamp": ts
                })

                # Store in TimeSeries
                # Create the rule automatically on first write
                try:
                    R.ts().add(f"ts:{sensor_id}:value", ts, value)
                except redis.exceptions.ResponseError:
                    # Time series doesn't exist, create it
                    R.ts().create(f"ts:{sensor_id}:value")
                    R.ts().add(f"ts:{sensor_id}:value", ts, value)

                # Publish a generic sensor update event with the full data
                R.publish("sensor_updates", json.dumps(sensor_info))
                print(f"Pushing update for {sensor_id}. Status: {status}")
                if status == "ANOMALY":
                    print(f"🚨 ANOMALY DETECTED for {sensor_id}: {value}")

                # Acknowledge the message so it's not processed again
                R.xack(STREAM_KEY, GROUP_NAME, entry_id)

if __name__ == "__main__":
    process_stream()
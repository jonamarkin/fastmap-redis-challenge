import time
import random
import json
from config import R

# A list of sensor dictionaries, all located in the Tuscany region of Italy.
SENSORS = [
    {"id": "sensor-001", "coords": [43.7228, 10.4017]}, 
    {"id": "sensor-002", "coords": [43.7696, 11.2558]},  
    {"id": "sensor-003", "coords": [43.8429, 10.5027]}, 
    {"id": "sensor-004", "coords": [43.3188, 11.3308]}
]

def setup_sensors():
    """Iterate through the SENSORS list and store static metadata for each in RedisJSON."""
    for sensor in SENSORS:
        sensor_id = sensor["id"]
        sensor_data = {
            "id": sensor_id,
            "type": "pressure",
            "location": {
                "lat": sensor["coords"][0],
                "lon": sensor["coords"][1]
            },
            "install_date": "2025-08-08"
        }
        # Use a unique key for each sensor
        R.json().set(f"sensor:{sensor_id}", "$", sensor_data)
        print(f"Sensor {sensor_id} configured.")

def run_sensors():
    """Continuously generate data for a random sensor and add it to a stream."""
    while True:
        # Choose a random sensor from the list
        sensor = random.choice(SENSORS)
        sensor_id = sensor["id"]

        reading = {
            "value": round(random.uniform(100.0, 105.0) + (random.random() > 0.90) * 20, 2),
            "timestamp": int(time.time() * 1000)
        }
        
        # XADD adds the reading to the stream 'raw_sensor_data'
        entry_id = R.xadd("raw_sensor_data", {"sensor_id": sensor_id, "value": reading["value"]})
        print(f"Sensor {sensor_id}: Sent reading {reading['value']}. Entry ID: {entry_id}")
        
        # Sleep for a short interval before the next reading
        time.sleep(1)

if __name__ == "__main__":
    setup_sensors()
    run_sensors()
from flask import Flask, render_template
from flask_socketio import SocketIO
import json
from config import R

app = Flask(__name__)
socketio = SocketIO(app)

@app.route('/')
def index():
    """Serve the main dashboard page."""
    return render_template('index.html')

@app.route('/api/sensors')
def get_sensors():
    """API endpoint to get all sensor data for initial map load."""
    sensor_keys = R.keys("sensor:*")
    sensors = []
    for key in sensor_keys:
        sensor_id = key.split(":")[1]
        
        # Get static data from RedisJSON
        sensor_info = R.json().get(key)
        
        # Get current state from Redis Hash
        sensor_state = R.hgetall(f"sensor_state:{sensor_id}")
        
        # Combine them
        sensor_info.update(sensor_state)
        sensors.append(sensor_info)
        
    return json.dumps(sensors)

def redis_pubsub_listener():
    """Listen to Redis Pub/Sub and emit alerts via WebSocket."""
    pubsub = R.pubsub()
    # Subscribe to the new, more general channel
    pubsub.subscribe("sensor_updates")
    for message in pubsub.listen():
        if message['type'] == 'message':
            print(f"Pushing sensor update to dashboard: {message['data']}")
            # Emit the data using a new event name
            socketio.emit('sensor_update', message['data'])

if __name__ == '__main__':
    # Start the Pub/Sub listener in a background thread
    socketio.start_background_task(target=redis_pubsub_listener)
    socketio.run(app, debug=True)
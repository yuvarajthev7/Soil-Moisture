import paho.mqtt.client as mqtt
import time
import json
import random

# --- Configuration ---
BROKER_ADDRESS = "localhost"
MQTT_TOPIC = "soil-moisture-topic"
DEVICE_ID = "SIMULATOR_01"

# --- Main Program ---
print("Starting simulator...")

# Create a new MQTT client
client = mqtt.Client()

# Connect to the broker
try:
    client.connect(BROKER_ADDRESS, 1883)
    print(f"Connected to broker at {BROKER_ADDRESS}")
except Exception as e:
    print(f"ERROR: Could not connect to broker at {BROKER_ADDRESS}.")
    print("Please ensure Mosquitto is running.")
    exit()

# Start a loop to publish data
try:
    while True:
        # 1. Generate a random moisture value
        moisture_value = random.randint(300, 900)

        # 2. Format as a JSON payload
        #    (We use json.dumps to convert a Python dict to a JSON string)
        payload = {
            "deviceId": DEVICE_ID,
            "moisture": moisture_value,
            "timestamp": int(time.time())
        }
        payload_str = json.dumps(payload)

        # 3. Publish the message
        result = client.publish(MQTT_TOPIC, payload_str)

        # Check if publish was successful
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"✅ Message PUBLISHED to '{MQTT_TOPIC}': {payload_str}")
        else:
            print(f"Failed to publish message, return code: {result.rc}")

        # 4. Wait for 5 seconds
        time.sleep(5)

except KeyboardInterrupt:
    print("\nSimulator stopped.")
finally:
    # Disconnect from the broker when stopping
    client.disconnect()
    print("Disconnected from broker.")

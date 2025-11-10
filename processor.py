import paho.mqtt.client as mqtt
import json
import psycopg2  # The library for_PostgreSQL
import datetime
import time

# --- Configuration ---
BROKER_ADDRESS = "localhost"
MQTT_TOPIC = "soil-moisture-topic"

# !!! YOUR NEON DB STRING IS PASTED HERE !!!
NEON_DB_STRING = "postgresql://neondb_owner:npg_YLXvxpE1Dlz7@ep-frosty-resonance-a942sgie-pooler.gwc.azure.neon.tech/neondb?sslmode=require&channel_binding=require"

# --- Alert Configuration ---
ALERT_THRESHOLDS = {
    "DRY_LIMIT": 450,  # Alert if moisture is BELOW this
    "WET_LIMIT": 850   # Alert if moisture is ABOVE this
}

# --- Database Functions ---

def get_db_conn():
    """Connects to the Neon database."""
    try:
        conn = psycopg2.connect(NEON_DB_STRING)
        return conn
    except Exception as e:
        print(f"DB Error: Could not connect. {e}")
        return None

def setup_database(conn):
    """Create the readings table if it doesn't exist."""
    print("Setting up database table...")
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS readings (
                id SERIAL PRIMARY KEY,
                device_id TEXT NOT NULL,
                moisture INTEGER NOT NULL,
                timestamp TIMESTAMP NOT NULL
            )
            ''')
        conn.commit()
        print("Database setup complete.")
    except Exception as e:
        print(f"DB Setup Error: {e}")


def insert_data(conn, device_id, moisture, timestamp):
    """Insert a new data reading into the database."""
    try:
        with conn.cursor() as cursor:
            dt_object = datetime.datetime.fromtimestamp(timestamp)
            cursor.execute("INSERT INTO readings (device_id, moisture, timestamp) VALUES (%s, %s, %s)",
                           (device_id, moisture, dt_object))
        conn.commit()
    except Exception as e:
        print(f"DB Error: Failed to insert data. {e}")

# --- Alerting Function ---

def check_thresholds(device_id, moisture):
    """Check moisture against thresholds and print alerts."""

    if moisture < ALERT_THRESHOLDS["DRY_LIMIT"]:
        print("\n" + "!"*40)
        print(f"  🚨 ALERT: SOIL IS TOO DRY! 🚨")
        print(f"  Device: {device_id} | Moisture: {moisture}")
        print("  Action:   Start irrigation.")
        print("!"*40 + "\n")

    elif moisture > ALERT_THRESHOLDS["WET_LIMIT"]:
        print("\n" + "="*40)
        print(f"  ⚠️  WARNING: SOIL IS TOO WET  ⚠️")
        print(f"  Device: {device_id} | Moisture: {moisture}")
        print(f"  Action:   Stop irrigation.")
        print("="*40 + "\n")

# --- MQTT Callback Functions ---

def on_connect(client, userdata, flags, rc):
    """Called when the client successfully connects to the broker."""
    if rc == 0:
        print(f"Connected successfully to broker at {BROKER_ADDRESS}")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    """Called every time a message is received on the subscribed topic."""
    global db_conn  # Use the global connection
    try:
        data = json.loads(msg.payload.decode('utf-8'))

        device_id = data.get("deviceId")
        moisture = data.get("moisture")
        timestamp = data.get("timestamp")

        if device_id and moisture is not None and timestamp:
            print(f"✅ Data Received: {device_id}, Moisture: {moisture}")

            # Check if connection is valid, otherwise reconnect
            if db_conn.closed:
                print("DB connection closed. Reconnecting...")
                db_conn = get_db_conn()

            if db_conn:
                insert_data(db_conn, device_id, moisture, timestamp)
                check_thresholds(device_id, moisture)
            else:
                print("Cannot insert data, no database connection.")

    except Exception as e:
        print(f"ERROR processing message: {e}")

# --- Main Program ---
print("Connecting to Neon database...")
db_conn = get_db_conn()

if db_conn:
    # Set up the database table
    setup_database(db_conn)

    # Configure and connect the MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(BROKER_ADDRESS, 1883)
    except Exception as e:
        print(f"ERROR: Could not connect to Mosquitto broker at {BROKER_ADDRESS}.")
        print("Please ensure Mosquitto is running.")
        exit()

    # Start the forever-loop
    print(f"Starting listener for topic '{MQTT_TOPIC}'... (Press CTRL+C to stop)")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nStopping listener...")
    finally:
        db_conn.close()
        client.disconnect()
        print("Disconnected from broker and database.")

else:
    print("FATAL: Could not connect to Neon database on startup. Exiting.")

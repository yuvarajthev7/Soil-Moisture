import streamlit as st
import psycopg2
import pandas as pd
import time

# --- Database Connection Function ---

@st.cache_resource  # Caches the connection for performance
def get_db_conn():
    """Establishes a connection to the Neon PostgreSQL database."""
    try:
        # Get the connection string from Streamlit's secrets
        conn_string = st.secrets["DB_CONNECTION_STRING"]
        conn = psycopg2.connect(conn_string)
        return conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None

# --- Data Fetching Function ---

@st.cache_data(ttl=10)  # Caches the data for 10 seconds
def fetch_data(_conn):
    """Fetches soil moisture data from the database."""
    try:
        # Re-establish connection if closed
        if _conn.closed:
            _conn = get_db_conn()

        with _conn.cursor() as cur:
            # Query to get data from the last 1 hour
            query = """
                SELECT
                    timestamp,
                    moisture
                FROM
                    readings
                WHERE
                    timestamp >= NOW() - INTERVAL '1 hour'
                ORDER BY
                    timestamp ASC;
            """
            cur.execute(query)
            data = cur.fetchall()

            # Convert fetched data to a Pandas DataFrame for easier plotting
            if data:
                df = pd.DataFrame(data, columns=['timestamp', 'moisture'])
                # Set the timestamp as the index, which st.line_chart understands
                df = df.set_index('timestamp')
                return df
            else:
                # Return an empty DataFrame if no data
                return pd.DataFrame(columns=['timestamp', 'moisture']).set_index('timestamp')

    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

# --- Main App ---

# Set page title and layout
st.set_page_config(page_title="Soil Moisture Dashboard", layout="wide")

# Title of the dashboard
st.title("🌱 Soil Moisture Real-Time Dashboard")

# Create a placeholder for our chart
placeholder = st.empty()

# --- Auto-Refreshing Loop ---
while True:
    # 1. Get database connection
    conn = get_db_conn()

    if conn:
        # 2. Fetch the data
        df = fetch_data(conn)

        # 3. Use the placeholder to draw/redraw the dashboard contents
        with placeholder.container():
            st.subheader(f"Live Moisture Readings (Last 1 Hour)")
            st.text(f"Last updated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

            if df is None:
                st.error("Could not fetch data.")
            elif df.empty:
                st.warning("No data found for the last 1 hour. Is the processor.py script running?")
            else:
                # 4. Display the chart
                st.line_chart(df)

    # 5. Wait for 10 seconds before refreshing
    time.sleep(10)

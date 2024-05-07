from kafka import KafkaConsumer
import json
import pandas as pd
import matplotlib.pyplot as plt 

# Kafka broker and topic
kafka_broker = "localhost:9092"
kafka_topic = "google-maps-traffic-data"

# Initialize Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=[kafka_broker],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='cold-chain-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Data storage
data = []

# Kafka consumer loop
# Kafka consumer loop with error handling
for message in consumer:
    eta_data = message.value
    
    # Use `get` method to safely access dictionary keys with default values
    origin = eta_data.get("origin", "Unknown")
    destination = eta_data.get("destination", "Unknown")
    eta_formatted = eta_data.get("eta_formatted", "N/A")
    timestamp = eta_data.get("timestamp", "N/A")

    # Print ETA information with error handling
    try:
        print(f"Origin: {origin}, Destination: {destination}, ETA: {eta_formatted}, Timestamp: {timestamp}")
    except KeyError as e:
        print(f"Key error occurred: {e}")

    # Append data to list
    data.append({
        "origin": origin,
        "destination": destination,
        "eta_formatted": eta_formatted,
        "timestamp": timestamp
    })


# Convert data to a DataFrame for further analysis or visualization
df = pd.DataFrame(data)

# Plot ETA over time
df["timestamp"] = pd.to_datetime(df["timestamp"])
df.sort_values("timestamp", inplace=True)

plt.plot(df["timestamp"], df["eta_in_seconds"], label="ETA in Seconds")
plt.xlabel("Time")
plt.ylabel("ETA (seconds)")
plt.title("ETA Over Time")
plt.legend()
plt.show()
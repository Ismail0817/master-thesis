import pandas as pd
import paho.mqtt.client as mqtt
import json
import time

# MQTT setup
broker_address = "broker.hivemq.com"  
client = mqtt.Client()
client.connect(broker_address, 1883, 60)

# Read CSV file
csv_file = "smoke_detection.csv"  # Replace with your CSV file path
df = pd.read_csv(csv_file, delimiter=';')  # Use semicolon as delimiter

# Publish data
for index, row in df.iterrows():
    utc = row['UTC']
    for col in df.columns:
        if col != 'UTC':
            topic = f"{col.lower().replace(' ', '_').replace('[', '').replace(']', '').replace('.', '')}"  # Clean topic name
            message = json.dumps({"utc": utc, col: row[col]})
            client.publish(topic, message)
            print(f"Published to {topic}: {message}")
    time.sleep(1)  # Wait for 1 second before sending the next row
    print("\n")

print("All data published.")
client.disconnect()

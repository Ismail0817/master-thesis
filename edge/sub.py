import paho.mqtt.client as mqtt
import json
import time

import requests

# Define the topics based on the column names
topics = [
    "temperaturec",
    "pm10",
    "pm25",
    "nc05",
    "nc10",
    "fire_alarm"
]

# Dictionary to store received data
received_data = {topic: [] for topic in topics}

# Callback when a message is received
def on_message(client, userdata, message):
    payload = json.loads(message.payload.decode("utf-8"))
    topic = message.topic
    received_data[topic].append(payload)
    print(f"Received message from {topic}: {payload}")

# MQTT setup
broker_address = "broker.hivemq.com"  # Replace with your broker address
client = mqtt.Client()
client.on_message = on_message

# Connect to the MQTT broker
client.connect(broker_address, 1883, 60)

# Subscribe to all topics
for topic in topics:
    client.subscribe(topic)
    print(f"Subscribed to {topic}")

# Start the MQTT loop in a non-blocking way
client.loop_start()

# Collect data for 5 seconds
time.sleep(5)

# Stop the MQTT loop
client.loop_stop()

# Merge data by utc
merged_data = {}

for topic, messages in received_data.items():
    for message in messages:
        utc = message['utc']
        if utc not in merged_data:
            merged_data[utc] = {}
        # Update merged data with message values for all topics
        merged_data[utc].update(message)

# Convert the merged data to a list of dictionaries sorted by utc
merged_list = [{'utc': utc, **values} for utc, values in merged_data.items()]
merged_list.sort(key=lambda x: x['utc'])

print("Merge data:")

# Print the merged data
print(json.dumps(merged_list, indent=4))

# Send merged data to API endpoint
api_endpoint = "http://192.168.10.145:5000/container_api" 
# headers = {"Content-Type": "application/json"}
# response = requests.post(api_endpoint, json=merged_list, headers=headers)

headers = {"Content-Type": "application/json"}
payload = {"message": merged_list}  # Wrap the list in a dictionary with key "data"
response = requests.post(api_endpoint, json=payload, headers=headers)

if response.status_code == 200:
    print("Data successfully sent to the API endpoint.")
else:
    print(f"Failed to send data to the API endpoint. Status code: {response.status_code}")

# Disconnect from the MQTT broker
client.disconnect()

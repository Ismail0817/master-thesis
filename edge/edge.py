import paho.mqtt.client as mqtt
import json
import time
from flask import Flask, request, jsonify
from threading import Thread
import os
import signal

import requests

app = Flask(__name__)

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

# MQTT client setup
client = mqtt.Client()
client.on_message = on_message

# Flask API to receive the time duration for data collection
@app.route('/collect', methods=['POST'])
def collect_data():
    data = request.json
    collection_time = data.get('collection_time', 10)  # Default to 10 seconds if not specified

    # Start a new thread to handle data collection
    thread = Thread(target=collect_mqtt_data, args=(collection_time,))
    thread.start()

    return jsonify({"status": "Data collection started", "collection_time": collection_time}), 200

def collect_mqtt_data(collection_time):
    # Connect to the MQTT broker
    broker_address = "broker.hivemq.com"  # Replace with your broker address
    client.connect(broker_address, 1883, 60)

    # Subscribe to all topics
    for topic in topics:
        client.subscribe(topic)
        print(f"Subscribed to {topic}")

    # Start the MQTT loop in a non-blocking way
    client.loop_start()

    # Collect data for the specified amount of time
    time.sleep(collection_time)

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

    print("Merged data:")

    # Print the merged data
    print(json.dumps(merged_list, indent=4))

    # Send merged data to API endpoint
    api_endpoint = "http://192.168.10.145:5000/container_api" 
    headers = {"Content-Type": "application/json"}
    payload = {"message": merged_list}  # Wrap the list in a dictionary with key "message"
    response = requests.post(api_endpoint, json=payload, headers=headers)

    if response.status_code == 200:
        print("Data successfully sent to the API endpoint.")
    else:
        print(f"Failed to send data to the API endpoint. Status code: {response.status_code}")

        print("Data collection completed and saved to merged_sensor_data.json")

    # Shutdown Flask server after data collection
    os.kill(os.getpid(), signal.SIGINT)

if __name__ == '__main__':
    # Run the Flask app
    app.run(host='0.0.0.0',port=5005)

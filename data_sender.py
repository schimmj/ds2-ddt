import json
import random
import paho.mqtt.client as mqtt
import time
import sys

# Define the MQTT broker address and port
BROKER_ADDRESS = "localhost"  # "mqtt-broker" when deploying instead of localhost
BROKER_PORT = 1883

DATA_URL = "demo_data/formatted_weather.json"

# Check if the topic was provided as a command-line argument
if len(sys.argv) != 2:
    print("Usage: python mqtt_reader.py <TOPIC>")
    exit()

TOPIC = sys.argv[1]


# MQTT-Client initialization
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

# Connect to the broker
try:
    client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
    print(f"Sender sucessfully connected to the mqtt-broker on {BROKER_ADDRESS}: {BROKER_PORT}")
    
except: 
    print("Error: Sender did not connect to the mqtt-broker!")



print("=" * 80 + "\n")

with open(DATA_URL, 'r') as file:
    data = json.load(file)




for item in data:
    # selected_topic = random.choice([WEATHER_TOPIC, TRAFFIC_TOPIC])
    selected_topic = TOPIC
    client.publish(selected_topic, json.dumps(item))
    print(f"Item sent to {selected_topic}: {item}")

    time.sleep(1)

# close connection
client.disconnect()

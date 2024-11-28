import json
import random
import paho.mqtt.client as mqtt
import time

# Define the MQTT broker address and port
BROKER_ADDRESS = "localhost"  # "mqtt-broker" when deploying instead of localhost
BROKER_PORT = 1883
WEATHER_TOPIC = "weather"
TRAFFIC_TOPIC = "traffic"
DATA_URL = "demo_data/weather_january.json"


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
    clean_data = [x['metdata'] for x in data]




for item in data:
    # selected_topic = random.choice([WEATHER_TOPIC, TRAFFIC_TOPIC])
    selected_topic = WEATHER_TOPIC
    
    client.publish(selected_topic, json.dumps(item['metdata']))
    print(f"Item sent to {selected_topic}: {item['metdata']}")

    time.sleep(2)

# close connection
client.disconnect()

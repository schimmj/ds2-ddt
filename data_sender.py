import json
import paho.mqtt.client as mqtt
import time

# Define the MQTT broker address and port
BROKER_ADDRESS = "localhost"  # "mqtt-broker" when deploying instead of localhost
BROKER_PORT = 1883
TOPIC = "your/topic"
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
    client.publish(TOPIC, json.dumps(item['metdata']))
    print(f"Item sent to {TOPIC}: {item['metdata']}")

    time.sleep(3)

# close connection
client.disconnect()

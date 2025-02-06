import paho.mqtt.client as mqtt
import sys

# Define the MQTT broker address and port
BROKER_ADDRESS = "localhost"
BROKER_PORT = 1883

# Check if the topic was provided as a command-line argument
if len(sys.argv) != 2:
    print("Usage: python mqtt_reader.py <TOPIC>")
    exit()

TOPIC = sys.argv[1]

# Callback function to handle incoming messages
def on_message(client, userdata, msg):
    print(f"Message received on topic '{msg.topic}': \n {msg.payload.decode()}")

# MQTT Client initialization
client = mqtt.Client()

# Assign the on_message callback
client.on_message = on_message

# Connect to the broker
try:
    client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
    print(f"Reader successfully connected to the MQTT broker on {BROKER_ADDRESS}:{BROKER_PORT}")
except Exception as e:
    print(f"Error: Reader could not connect to the MQTT broker! {e}")
    exit()

# Subscribe to the desired topic
client.subscribe(TOPIC)
print(f"Subscribed to topic '{TOPIC}'")

# Start the loop to process messages
try:
    print("Waiting for messages. Press Ctrl+C to exit.")
    client.loop_forever()
except KeyboardInterrupt:
    print("\nDisconnecting from the broker...")
    client.disconnect()
    print("Disconnected.")

import glob
import json
import paho.mqtt.client as mqtt
from data_queue import add_to_queue
import time
import threading
from data_queue import process_batch

# MQTT Configuration
MQTT_BROKER = "localhost"  # Change to "mqtt-broker" when deploying instead of localhost
MQTT_PORT = 1883
TOPIC = "your/topic"

TIMEOUT = 10

last_message_time = None

def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8")
    data = json.loads(payload)
    add_to_queue(data, client)  # Push received message to the queue
    global last_message_time
    last_message_time = time.time()

def start_mqtt_listener():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT)
    client.subscribe(TOPIC)
    client.loop_start()
    return client

def start_mqtt_sender():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT)
    client.subscribe(TOPIC)
    client.loop_start()
    global last_message_time
    last_message_time = time.time()
    threading.Thread(target=check_message_timeout, args=(client,), daemon=True).start()
    return client
    


def check_message_timeout(client):
    global last_message_time
    while True:
        time_since_last_msg = time.time() - last_message_time
        if time_since_last_msg > TIMEOUT:
            process_batch(client)
        time.sleep(TIMEOUT / 2)    



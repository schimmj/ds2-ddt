"""
Module for handling MQTT messages and processing them into queues.

This script defines handlers for weather and traffic messages received via MQTT
and processes these messages into topic-specific data queues for further handling.
The MQTT broker connection is managed by the `MQTTHandler` class.
"""
import os
import time
from typing import Callable, Dict, List
from mqtt import MQTTHandler
from data_queue import DataQueue
from paho.mqtt.client import MQTTMessage
import json
from dotenv import load_dotenv
from validation import GXInitializer

load_dotenv()

# Configuration via environment variables
BROKER = os.getenv("BROKER", "localhost")  # Address of the MQTT broker
PORT = int(os.getenv("PORT", 1883))        # Port to connect to the MQTT broker
 
def main():
    """
    Main function to start the MQTT client and process incoming messages.

    This function initializes the MQTTHandler with topic handlers for weather and traffic messages.
    It then starts the MQTT client and waits for incoming messages in an infinite loop.
    """
    mqtt_handler = MQTTHandler(BROKER, PORT)
    gx_initializer = GXInitializer()
    
    try:
        mqtt_handler.start()
        print("MQTT client running. Press Ctrl+C to exit.")
        while True:
            time.sleep(1)  # Keep the main thread alive
    except KeyboardInterrupt:
        print("Exiting...")
        mqtt_handler.stop()


if __name__ == "__main__":
    main()

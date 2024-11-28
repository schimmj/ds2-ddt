"""
Module for handling MQTT messages and processing them into queues.

This script defines handlers for weather and traffic messages received via MQTT
and processes these messages into topic-specific data queues for further handling.
The MQTT broker connection is managed by the `MQTTHandler` class.
"""

import time
from typing import Callable, Dict, List
from mqtt_handler import MQTTHandler
from data_queue import DataQueue
from paho.mqtt.client import MQTTMessage
import json

# Configuration
BROKER = "localhost"  # Address of the MQTT broker
PORT = 1883           # Port to connect to the MQTT broker
WEATHER_TOPIC = "weather"  # Topic for weather-related messages
TRAFFIC_TOPIC = "traffic"  # Topic for traffic-related messages


# Global variable for storing data queues
queues: List[DataQueue] = []





def handle_weather_messages(client, userdata, msg: MQTTMessage):
    """
    Handles messages received on the weather topic.

    This function processes messages received on the weather topic by adding them to the relevant data queue.

    Args:
        client: The MQTT client instance.
        userdata: User-defined data of any type.
        msg (MQTTMessage): The message object containing topic and payload.
    """
    create_queue_or_add_message(msg, batch_size=10)

def handle_traffic_messages(client, userdata, msg: MQTTMessage):
    """
    Handles messages received on the traffic topic.

    This function processes messages received on the traffic topic by adding them to the relevant data queue.

    Args:
        client: The MQTT client instance.
        userdata: User-defined data of any type.
        msg (MQTTMessage): The message object containing topic and payload.
    """
    create_queue_or_add_message(msg, batch_size=5)
    
    
    
    

def create_queue_or_add_message(msg: MQTTMessage, batch_size: int):
    """
    Adds a message to an existing queue or creates a new queue if none exists for the topic.

    Args:
        msg (MQTTMessage): The message object containing topic and payload.
        batch_size (int): The maximum number of messages in a queue before it is processed.
    """
    global queues
    
    try:
        decoded_payload = msg.payload.decode("utf-8") 
        json_msg = json.loads(decoded_payload)
    except UnicodeDecodeError as e:
        print(f"Failed to decode message payload: {e}")
        return
    
    
    queue = next((q for q in queues if q.topic == msg.topic), None)
    
    if queue is None:
        # Create a new queue if no queue exists for the topic
        new_queue = DataQueue(topic=msg.topic, batch_size=batch_size)
        new_queue.add(json_msg)
        queues.append(new_queue)
        print(f"Created new queue for topic: {msg.topic}")
    else:
        # Add the message to the existing queue
        queue.add(json_msg)
        print(f"Added message to existing queue for topic: {msg.topic}, at position {len(queue.queue)}")

def main():
    """
    Main function to start the MQTT client and process incoming messages.

    This function initializes the MQTTHandler with topic handlers for weather and traffic messages.
    It then starts the MQTT client and waits for incoming messages in an infinite loop.
    """
    topic_handlers: Dict[str, Callable] = {
        WEATHER_TOPIC: handle_weather_messages,
        TRAFFIC_TOPIC: handle_traffic_messages
    }
    
    # Initialize the MQTTHandler
    mqtt_handler = MQTTHandler(
        broker=BROKER,
        port=PORT,
        topic_handlers=topic_handlers
    )

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

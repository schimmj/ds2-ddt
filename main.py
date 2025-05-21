"""
Module for handling MQTT messages and processing them into queues.
"""
import os
import time
from mqtt import MqttClient, MqttPublisher, TopicRouter 
from dotenv import load_dotenv
from result_handler import ResultHandler
from validation import GXInitializer
import paho.mqtt.client as mqtt

load_dotenv(override=True)

# Configuration via environment variables
BROKER = os.getenv("BROKER", "localhost")  # Address of the MQTT broker
PORT = int(os.getenv("PORT", 1883))        # Port to connect to the MQTT broker
 
def main():
    """
    Main function to start the MQTT client and process incoming messages.
    """
    gx_initializer = GXInitializer()
            
    
    client  = MqttClient(broker=BROKER, port=PORT)
    publisher = MqttPublisher(client)                   # hand same client to RH
    # # tell *every* newly‑created ResultHandler to use the shared publisher
    ResultHandler.set_default_publisher(publisher.publish)
    
    router  = TopicRouter(client)                       # creates pipelines



    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        client.stop()





if __name__ == "__main__":
    main()

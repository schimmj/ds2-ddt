"""
Module for handling MQTT messages and processing them into queues.
"""
import os
import time
from mqtt import MqttClient, MqttPublisher, TopicRouter 
from dotenv import load_dotenv
from validation import GXInitializer

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
    router  = TopicRouter(client)                       # creates pipelines
    publisher = MqttPublisher(client)                   # hand same client to RH

    # # tell *every* newly‑created ResultHandler to use the shared publisher
    # BatchPipeline.set_default_publisher(publisher.publish)  # tiny helper class‑method

    client.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        client.stop()





if __name__ == "__main__":
    main()

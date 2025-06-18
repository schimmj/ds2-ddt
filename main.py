"""
Module for handling MQTT messages and processing them into queues.
"""
import os
import time
from mqtt import MqttClient, MqttPublisher 
from dotenv import load_dotenv
from result_handler import ResultHandler
from validation import GXInitializer
import paho.mqtt.client as mqtt
import uvicorn
from batch import PipelineManager
from api.api_server import app

load_dotenv(override=True)

# Configuration via environment variables
BROKER = os.getenv("BROKER", "localhost")  # Address of the MQTT broker
PORT = int(os.getenv("PORT", 1883))        # Port to connect to the MQTT broker
 
def main():
   # any one-time initialization
    gx_initializer = GXInitializer()

    # --- MQTT setup ---
    client = MqttClient(broker=BROKER, port=PORT)

    # hook up your ResultHandler to publish back over MQTT
    publisher = MqttPublisher(client)
    ResultHandler.set_default_publisher(publisher.publish)

    # --- PipelineManager (wires pipelines into the MQTT client) ---
    manager = PipelineManager(
        cfg_path="generated_mqtt_config.json",
        mqtt_client=client
    )

    # inject into FastAPI
    app.state.manager = manager

    # start listening into MQTT (non-blocking)
    client.start()

    # launch the HTTP server
    uvicorn.run(app, host="0.0.0.0", port=8000)





if __name__ == "__main__":
    main()

# SPDX-FileCopyrightText: 2025 - 2025 Software GmbH, Darmstadt, Germany and/or its subsidiaries and/or its affiliates
# SPDX-License-Identifier: Apache-2.0

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
        cfg_path="./config",
        mqtt_client=client
    )

    # --- API server setup ---#
    app.state.manager = manager
    app.state.gx = gx_initializer

    # start listening into MQTT (non-blocking)
    client.start()

    # launch the HTTP server
    uvicorn.run(app, host="0.0.0.0", port=8000)





if __name__ == "__main__":
    main()

# SPDX-FileCopyrightText: 2025 - 2025 Software GmbH, Darmstadt, Germany and/or its subsidiaries and/or its affiliates
# SPDX-License-Identifier: Apache-2.0

# mqtt_publisher.py
from mqtt import MqttClient
class MqttPublisher:
    """Simple façade injected into ResultHandler so it can publish without
    importing MQTT logic or creating extra clients."""
    def __init__(self, client):
        self._client = client
    def publish(self, topic: str, obj):
        self._client.publish(topic, obj)




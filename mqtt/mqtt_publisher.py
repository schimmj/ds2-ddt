# mqtt_publisher.py
class MqttPublisher:
    """Simple façade injected into ResultHandler so it can publish without
    importing MQTT logic or creating extra clients."""
    def __init__(self, client):
        self._client = client
    def publish(self, topic: str, obj): self._client.publish(topic, obj)




# mqtt_client.py
import json, paho.mqtt.client as mqtt
from typing import Callable, List

class MqttClient:
    """Tiny wrapper around paho‑mqtt that emits
    (topic:str, payload:dict) events to listeners."""
    
    def __init__(self, broker: str, port: int):
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._client.on_message = self._raw_on_message
        self._client.connect(broker, port)
        print(f"MQTT Client connected to {broker}:{port}")
        self._listeners: List[Callable[[str, dict], None]] = []

    # --- public transport API ------------------------------------------------
    def subscribe(self, topic: str) -> None:        self._client.subscribe(topic)
    def publish(self, topic: str, obj) -> None:     self._client.publish(topic, json.dumps(obj))
    def add_listener(self, fn: Callable[[str, dict], None]) -> None: self._listeners.append(fn)
    def start(self): self._client.loop_start()
        
    def stop(self):  self._client.loop_stop(); self._client.disconnect()

    # --- internal ------------------------------------------------------------
    def _raw_on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload)
        except json.JSONDecodeError:
            print(f"⚠️  non‑JSON on {msg.topic}: {msg.payload!r}")
            return
        for fn in self._listeners: fn(msg.topic, payload)

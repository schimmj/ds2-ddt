# mqtt_client.py
import json, paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from typing import Callable, List
import time

class MqttClient:
    """Tiny wrapper around paho‑mqtt that emits
    (topic:str, payload:dict) events to listeners."""
    
    
    def __init__(self, broker: str, port: int):
        self._client = mqtt.Client(CallbackAPIVersion.VERSION2)
        self._client.on_message = self._raw_on_message
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = lambda *args: print("MQTT Client disconnected")
        # self._client.on_publish = lambda *args: print("MQTT Client published")
        self._connected = False
        self._client.connect(broker, port)
        self._client.loop_start()
        print(f"✔ MQTT Client connected to {broker}:{port}")
        self._listeners: List[Callable[[str, dict], None]] = []

    def _on_connect(self, client, userdata, flags, rc, *args):
        if rc == 0:
            self._connected = True
        else:
            print(f"✖ Failed to connect, rc={rc}")
            
            
            
    # --- public transport API ------------------------------------------------
    def subscribe(self, topic: str) -> None:        self._client.subscribe(topic)
    def publish(self, topic: str, obj) -> None:
        print(f"📬 Publishing: {topic}: {obj!r}")
        while not self._connected:
            time.sleep(0.1)
        msg_info = self._client.publish(topic, json.dumps(obj))
    def add_listener(self, fn: Callable[[str, dict], None]) -> None: self._listeners.append(fn)
    def start(self): self._client.loop_start()
        
    def stop(self):  self._client.loop_stop(); self._client.disconnect()

    # --- internal ------------------------------------------------------------
    def _raw_on_message(self, client, userdata, msg):
        print(f"📬 {msg.topic}: {msg.payload!r}")
        try:
            payload = json.loads(msg.payload)
        except json.JSONDecodeError:
            print(f"⚠️  non‑JSON on {msg.topic}: {msg.payload!r}")
            return
        for fn in self._listeners: fn(msg.topic, payload)

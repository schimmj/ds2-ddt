# topic_router.py
from config import ConfigLoader
from batch import BatchPipeline
from mqtt import MqttClient

class TopicRouter:
    """Instantiate one BatchPipeline per *logical* topic and forward messages."""
    
    def __init__(self, client: MqttClient, cfg_path="generated_mqtt_config.json"):
        self._client = client
        raw_cfg = ConfigLoader().load_config(cfg_path)["topics"]
        self._match_table = {}          # raw MQTT topic → BatchPipeline

        for logical_name, cfg in raw_cfg.items():
            pipeline = BatchPipeline(logical_name, batch_size=cfg["batch_size"])
            raw = cfg["subscribe"]
            self._match_table[raw] = pipeline
            client.subscribe(raw)
            print(f"🔌  Subscribed to → {raw}")

        client.add_listener(self._dispatch)

    def _dispatch(self, raw_topic: str, payload: dict):
        pipeline = self._match_table.get(raw_topic)
        if pipeline:
            pipeline.add(payload)
        else:
            print(f"⚠️  No pipeline mapped for {raw_topic}")

import json
import logging
from re import sub
from typing import Any, Callable, Dict, Optional

from batch import BatchPipeline
from config import config_loader, config_provider
from config.config_loader import ConfigLoader
from mqtt import MqttClient  # your existing MQTT adapter
from config import ConfigProvider



class PipelineManager:
    """
    Loads all BatchPipelines from a JSON config file, keeps them in a dict,
    and (optionally) wires them into an MqttClient by subscribing & registering
    per-topic handlers.
    """

    def __init__(self, cfg_path: str, mqtt_client: Optional[MqttClient] = None):
        self._pipelines: Dict[str, BatchPipeline] = {}
        config_provider = ConfigProvider()

        self._load_pipelines(config_provider.mqtt())

        if mqtt_client:
            for topic, config in config_provider.mqtt().get("topics",{}).items():
                topic_to_subscribe = config.get("subscribe", topic)
                mqtt_client.subscribe(topic_to_subscribe)
                mqtt_client.add_listener(self._make_handler(topic))
                print(f"🔌 Subscribed to MQTT topic: {topic_to_subscribe}")

    def _load_pipelines(self, cfg: Dict[str, Any]) -> None:
        
        for topic, config in cfg.get("topics", {}).items():
            batch_size = config.get("batch_size", 50)
            topic_to_subscribe = config.get("subscribe", topic)
            config_name = config.get("validation_config", None)
            pipeline = BatchPipeline(topic=topic_to_subscribe, config_name= config_name, batch_size=batch_size)
            self._pipelines[topic_to_subscribe] = pipeline

    def _make_handler(self, topic: str) -> Callable[[str, dict], None]:
        """
        Returns a callback that can be passed to MqttClient.add_listener().
        It filters on `topic` and forwards matching messages to the pipeline.
        """
        def _handler(raw_topic: str, payload: dict) -> None:
            if raw_topic == topic:
                self._pipelines[topic].add(payload)
        return _handler

    def handler_for(self, topic: str) -> Callable[[str, dict], None]:
        """
        Expose the same per-topic handler for use by other transports (e.g. HTTP).
        """
        if topic not in self._pipelines:
            raise KeyError(f"No pipeline configured for topic '{topic}'")
        return self._make_handler(topic)

    def dispatch(self, topic: str, payload: dict) -> bool:
        """
        Legacy-style dispatch: look up the pipeline and enqueue payload.
        Returns True if dispatched, False if no pipeline for that topic.
        """
        pipeline = self._pipelines.get(topic)
        if not pipeline:
            return False
        pipeline.add(payload)
        return True

    @property
    def raw_topics(self):
        """List of all topics this manager knows about."""
        return list(self._pipelines.keys())
    

    def get_pipeline(self, topic: str) -> BatchPipeline:
        """
        Get the BatchPipeline for a given topic.
        Raises KeyError if no pipeline is configured for that topic.
        """
        if topic not in self._pipelines:
            raise KeyError(f"No pipeline configured for topic '{topic}'")
        return self._pipelines[topic]


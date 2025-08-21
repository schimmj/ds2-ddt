#batch/pipeline_manager.py
from curses import raw
from typing import Any, Callable, Dict, Optional
from threading import RLock
from batch import BatchPipeline
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
        self._lock  = RLock()
        self._mqtt_client = mqtt_client
        self._listeners : Dict[str, Callable[[str, dict], None]] = {}

        config_provider = ConfigProvider()

        self._apply_mqtt_config(config_provider.mqtt())


    def _apply_mqtt_config(self, mqtt_cfg: Dict[str, Any]) -> None:
        """
        Apply MQTT configuration to the MqttClient if available.
        Subscribes to all topics defined in the config.
        """
        with self._lock:
            # compute the new desired topics
            desired_topics = {}
            for topic, config in mqtt_cfg.get("topics", {}).items():
                topic_to_subscribe = config.get("subscribe", topic)
                desired_topics[topic_to_subscribe] = {
                    "batch_size": config.get("batch_size",50),
                    "validation_config": config.get("validation_config"),
                    "raw_topic": topic
                }
            
            #remove pipelines that are no longer in the config
            for existing in list(self._pipelines.keys()):
                if existing not in desired_topics:
                    self._pipelines.pop(existing, None)
                    if self._mqtt_client:
                        self._mqtt_client.unsubscribe(existing)

            # add or update pipelines based on the config
            for desired_topic, desired_config in desired_topics.items():
                if desired_topic not in self._pipelines:
                    self._pipelines[desired_topic] = BatchPipeline(
                        topic= desired_topic,
                        config_name=desired_config["validation_config"],
                        batch_size=desired_config["batch_size"]
                    )

                    if self._mqtt_client:
                        self._mqtt_client.subscribe(desired_topic)
                        if desired_topic not in self._listeners:
                            handler = self._make_handler(desired_topic)
                            self._listeners[desired_topic] = handler
                            self._mqtt_client.add_listener(handler)
    
    def reload_from_provider(self, config_provider: ConfigProvider) -> None:
        """
        Reloads the pipelines from the given ConfigProvider.
        This is useful for dynamic reconfiguration without restarting the service.
        """ 
        self._apply_mqtt_config(config_provider.mqtt())

   
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
    
    @property
    def pipelines(self) -> Dict[str, BatchPipeline]:
        """
        Expose the internal pipelines dict.
        This is useful for other components that need to access pipelines directly.
        """
        return self._pipelines
    

    def get_pipeline(self, topic: str) -> BatchPipeline:
        """
        Get the BatchPipeline for a given topic.
        Raises KeyError if no pipeline is configured for that topic.
        """
        if topic not in self._pipelines:
            raise KeyError(f"No pipeline configured for topic '{topic}'")
        return self._pipelines[topic]


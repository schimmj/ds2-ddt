import json
from typing import Dict
import paho.mqtt.client as mqtt
from data_queue import DataQueue
from config import ConfigLoader

class MQTTHandler:

    def __init__(self, broker: str, port: int):
        self.broker = broker
        self.port = port
        self.queues: Dict[str, DataQueue] = {}  # Dictionary to store DataQueue objects per topic.
        self.config = ConfigLoader().load_config('generated_mqtt_config.json')['topics']
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.connect(self.broker, self.port)
        
        self._setup_subscriptions()
            
    def _setup_subscriptions(self):
        """
        Create a data queue for each topic and subscribe to its raw topic.
        """
        for topic_name, topic_config in self.config.items():
            self.queues[topic_name] = DataQueue(topic_name, batch_size=topic_config['batch_size'])
            sub_topic = topic_config['subscribe']
            self.client.subscribe(sub_topic)
            # Add a callback to handle incoming messages on this topic.
            self.client.message_callback_add(
                sub_topic,
                lambda client, userdata, msg, t=topic_name: self._handle_message(msg, t)
            )
    
    def _handle_message(self, msg, topic_name: str):
        """
        Process an incoming MQTT message: Converting message from JSON and adding it to the corresponding queue.
        """
        try:
            data = json.loads(msg.payload)
            queue = self.queues[topic_name]
            queue.add(data)
            print(f"Message received on topic \"{topic_name}\": {data} at position {len(queue.queue)}")
        except Exception as e:
            print(f"Error processing message: {e}")
    
    def start(self):
        print("Starting MQTT client...")
        self.client.loop_start()
    
    def publish_results(self, validated_topic, results):
        result_json = json.dumps(results, default=str)
        message_info = self.client.publish(validated_topic, result_json)
        message_info.wait_for_publish(5)
    
    def publish_alarm(self, alarm_topic, message):
        self.client.publish(alarm_topic, "Alarm: " + message)
    
    def stop(self):
        """Stop the MQTT client loop and disconnect from the broker."""
        self.client.loop_stop()
        self.client.disconnect()
        print("MQTT client stopped.")

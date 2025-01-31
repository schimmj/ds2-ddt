import json
import time
import yaml
from typing import Callable, Dict, Any
import paho.mqtt.client as mqtt
from data_queue import DataQueue
from config import ConfigLoader

class MQTTHandler:
    TIMEOUT = 30  # Timeout for inactivity
    
    def __init__(self, 
                 broker: str, 
                 port: int):
        """
        Initialize the MQTTHandler.
        
        :param broker: MQTT broker address.
        :param port: MQTT broker port.
        :param topic: Subscription topic.
        :param alarm_topic: Alarm publication topic.
        :param validation_topic: Validation results publication topic.
        :param data_queue: DataQueue instance for processing data.
        """
        self.broker = broker
        self.port = port
        self.queues: Dict[str, DataQueue] = {}
        self.config = ConfigLoader().load_config('mqtt_config.yaml')['topics']
        
        # Initialize MQTT client
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.connect(self.broker, self.port)
        
        self._setup_subscriptions()
            
    def _setup_subscriptions(self):
        for topic_name, topic_config in self.config.items():
            # Create queue for topic
            self.queues[topic_name] = DataQueue(topic_name,
                batch_size=topic_config['batch_size']
            )
            # Subscribe to raw topic
            sub_topic = topic_config['subscribe']
            self.client.subscribe(sub_topic)
            self.client.message_callback_add(
                sub_topic,
                lambda c, u, msg, t=topic_name: self._handle_message(msg, t)
            )
    
    def _handle_message(self, msg, topic_name: str):
        try:
            data = json.loads(msg.payload)
            queue = self.queues[topic_name]
            config = self.config[topic_name]            
            queue.add(data)
        except Exception as e:
            print(f"Error processing message: {e}")
    
    def start(self):
        """Start the MQTT client."""
        print("Starting MQTT client...")
        self.client.loop_start()
        
        # Start a thread to monitor for message timeouts
        # threading.Thread(target=self._check_message_timeout, daemon=True).start()
    
    def _check_message_timeout(self):
        """Check for message timeout and process batch if necessary."""
        while True:
            if time.time() - self.last_message_time > self.TIMEOUT:
                self.data_queue.process_batch()
            time.sleep(self.TIMEOUT / 2)
    
    def publish_results(self, results, publish_topic):
        """Publish validation results."""
        result_json = json.dumps(results, default=str)
        message_info = self.client.publish(publish_topic, result_json)
        message_info.wait_for_publish(5)
        print(f"Validation results published on {publish_topic}")
    
    def publish_alarm(self, alarm, alarm_topic):
        """Publish an alarm message."""
        alarm_json = json.dumps(alarm, default=str)
        try:
            message_info = self.client.publish(alarm_topic, alarm_json)
            message_info.wait_for_publish(5)
            print(f"Alarm published on topic \"{alarm_topic}\": {alarm}")
        except Exception as e:
            print(f"Failed to publish alarm: {e}")
    
    def stop(self):
        """Stop the MQTT client."""
        self.client.loop_stop()
        self.client.disconnect()
        print("MQTT client stopped.")

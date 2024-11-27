import json
import time
import threading
from typing import Callable, Dict
import paho.mqtt.client as mqtt
from data_queue import DataQueue

class MQTTHandler:
    TIMEOUT = 30  # Timeout for inactivity
    
    def __init__(self, 
                 broker: str, 
                 port: int,
                 topic_handlers: Dict[str, Callable]):
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
        
        
        # Initialize MQTT client
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.connect(self.broker, self.port)
        
        # Subscribe and add topic specific callbacks
        for topic, handler in topic_handlers.items():
            self.client.subscribe(topic)
            self.client.message_callback_add(topic, handler)
        
        
        

    
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
    
    def publish_results(self, results):
        """Publish validation results."""
        result_json = json.dumps(results, default=str)
        message_info = self.client.publish(self.validation_topic, result_json)
        published = message_info.wait_for_publish(5)
        print(f"Validation results published: {published}")
    
    def publish_alarm(self, alarm):
        """Publish an alarm message."""
        alarm_json = json.dumps(alarm, default=str)
        try:
            message_info = self.client.publish(self.alarm_topic, alarm_json)
            message_info.wait_for_publish(5)
            print(f"Alarm published on topic \"{self.alarm_topic}\": {alarm}")
        except Exception as e:
            print(f"Failed to publish alarm: {e}")
    
    def stop(self):
        """Stop the MQTT client."""
        self.client.loop_stop()
        self.client.disconnect()
        print("MQTT client stopped.")

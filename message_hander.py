import json
import time 
from data_queue import DataQueue
 
 
 
def _on_message(self, client, userdata, msg):
    """Handle incoming MQTT messages."""
    payload = msg.payload.decode("utf-8")
    data = json.loads(payload)
    DataQueue.add(data)  # Add data to the queue
    self.last_message_time = time.time()  # Update last message timestamp
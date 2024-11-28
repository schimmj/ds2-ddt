import pandas as pd
from validation.gx_validation import validate_batch


class DataQueue:
    
    def __init__(self, topic: str, batch_size: int):
        self.queue = []
        self.topic = topic
        self.batch_size = batch_size
        
    def add(self, data):
        """Add data to the queue."""
        self.queue.append(data)
        if len(self.queue) >= self.batch_size:
            self.process_batch()
    
    def process_batch(self):
        """Process and validate a batch of data."""
        if not self.queue:
            from mqtt_handler import MQTTHandler
            MQTTHandler.publish_alarm("Warning: Sensor is not sending anymore!")
        else:
            df: pd.DataFrame = pd.DataFrame(self.queue)
            validation_results = validate_batch(df, self.topic)
        self.queue = []  # Clear the queue

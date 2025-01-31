import pandas as pd
import result_handler
from validation.gx_validation import validate_batch
from data_correction import DataCorrection
from result_handler import ResultHandler


class DataQueue:
    
    def __init__(self, topic: str, batch_size: int):
        self.queue = []
        self.topic = topic
        self.batch_size = batch_size
        self.resutl_handler = ResultHandler()
        
    def add(self, data):
        """Add data to the queue."""
        self.queue.append(data)
        if len(self.queue) >= self.batch_size:
            self.process_batch()
    
    def process_batch(self):
        """Process and validate a batch of data."""
        df: pd.DataFrame = pd.DataFrame(self.queue)
        validation_results = validate_batch(df, self.topic)
        
        self.resutl_handler.handle_results(validation_results, df)
            
        self.queue = []  # Clear the queue
        

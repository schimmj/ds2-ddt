import pandas as pd
from validation.gx_validation import validate_batch
from result_handler import ResultHandler 

class DataQueue:
    """A simple queue that collects data and processes it in batches."""
    
    def __init__(self, topic: str, batch_size: int):
        self.queue = []
        self.topic = topic
        self.batch_size = batch_size
        self.result_handler = ResultHandler(topic)

    def add(self, data):
        # Add new data to the queue.
        self.queue.append(data)
        if len(self.queue) >= self.batch_size:
            self.process_batch()

    def process_batch(self):
        # Convert the collected data into a DataFrame.
        df = pd.DataFrame(self.queue)

        validation_results = validate_batch(df, self.topic)

        self.result_handler.handle_results(validation_results, df)

        self.queue = []

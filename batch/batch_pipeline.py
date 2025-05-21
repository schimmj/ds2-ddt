# batch_pipeline.py
from .data_queue import DataQueue
from .batch_validator import BatchValidator
from result_handler import ResultHandler
import pandas as pd

class BatchPipeline:
    """Glue DataQueue → BatchValidator → ResultHandler."""

    def __init__(self, topic: str, batch_size: int):
        self.validator = BatchValidator(topic)
        self.result_handler = ResultHandler(topic)
        self.queue = DataQueue(batch_size, self._process)

    # external API: just forward rows to the queue
    def add(self, row: dict) -> None:
        self.queue.add(row)

    # internal callback
    def _process(self, df: pd.DataFrame) -> None:
        validation_results = self.validator(df)
        self.result_handler.handle(validation_results, df)

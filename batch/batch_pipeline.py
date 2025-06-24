# SPDX-FileCopyrightText: 2025 - 2025 Software GmbH, Darmstadt, Germany and/or its subsidiaries and/or its affiliates
# SPDX-License-Identifier: Apache-2.0

# batch_pipeline.py
from .data_queue import DataQueue
from .batch_validator import BatchValidator
from result_handler import ResultHandler
import pandas as pd

class BatchPipeline:
    """Glue DataQueue → BatchValidator → ResultHandler."""

    def __init__(self, topic:str,  config_name: str, batch_size: int):
        self.validator = BatchValidator(config_name)
        self.result_handler = ResultHandler(topic, config_name)
        self.queue = DataQueue(batch_size, self._process)

 
    def add(self, row: dict) -> None:
        self.queue.add(row)
        print(f"Added row to queue for topic '{self.validator.config_name}': {row}")

    # internal callback
    def _process(self, df: pd.DataFrame) -> None:
        '""Process a DataFrame asynchronously. After a DataQueue batch is ready."""'
        validation_results = self.validator(df)
        self.result_handler.handle(validation_results, df)


    def process_sync(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process a DataFrame synchronously. When a HTTP request comes in, we can use this to process the data immediately."""
        validation_results = self.validator(df)
        cleaned_df = self.result_handler.handle(validation_results, df)
        return cleaned_df


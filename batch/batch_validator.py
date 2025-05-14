# batch_validator.py
from validation.gx_validation import validate_batch
import pandas as pd

class BatchValidator:
    def __init__(self, topic: str):
        self.topic = topic

    def __call__(self, df: pd.DataFrame) -> dict:
        """Return Great‑Expectations validation results."""
        return validate_batch(df, self.topic)

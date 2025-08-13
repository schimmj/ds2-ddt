# batch_validator.py
from validation.gx_validation import validate_batch
import pandas as pd

class BatchValidator:
    def __init__(self, config_name: str):
        self.config_name = config_name

    def __call__(self, df: pd.DataFrame):
        """Return Great‑Expectations validation results."""
        return validate_batch(df, self.config_name)

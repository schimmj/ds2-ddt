# data_queue.py
import pandas as pd
from collections.abc import Callable

class DataQueue:
    """Collect rows and fire a callback when a full batch is ready."""

    def __init__(self, batch_size: int, on_batch_ready: Callable[[pd.DataFrame], None]):
        self._batch_size = batch_size
        self._on_batch_ready = on_batch_ready
        self._buffer: list[dict] = []

    def add(self, row: dict) -> None:
        """Add a new row.  When the buffer reaches batch_size,
        emit a DataFrame to the callback and clear the buffer."""
        self._buffer.append(row)
        if len(self._buffer) >= self._batch_size:
            df = pd.DataFrame(self._buffer)
            self._on_batch_ready(df)
            self._buffer.clear()



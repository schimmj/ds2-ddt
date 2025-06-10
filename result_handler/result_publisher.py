"""
ResultPublisher
===============

Row‑wise publisher for *cleaned* DataFrames.  It flattens each row so the
consumer sees both the original **and** the cleaned value:

    "temperature.raw":    17.5,
    "temperature.cleaned": 18.1,
    …

A small `delay` parameter is optional for rate‑limiting bursty output.
"""
from __future__ import annotations

from curses import raw
from typing import Callable, Dict, List
import time
import json

import pandas as pd


class ResultPublisher:
    """Publish a cleaned DataFrame row‑by‑row via the injected `publish`."""

    def __init__(
        self,
        topic_cfg: Dict,
        publish: Callable[[str, dict], None],
        delay: float = 1.0,                    # seconds between messages
    ) -> None:
        self._result_topic: str = topic_cfg["publish"]["validated"]
        self._timestamp_attribute: str = topic_cfg["timestamp_attribute"]
        self._publish = publish

    # ------------------------------------------------------------------ #
    #  public API
    # ------------------------------------------------------------------ #
    def emit(
        self,
        cleaned_df: pd.DataFrame,
        raw_df: pd.DataFrame
    ) -> None:
        """
        Publish each row of `cleaned_df` together with its raw counterpart.

        Both dataframes must have identical indices and columns.
        """
        raw_json_rows: List[Dict] = json.loads(raw_df.to_json(orient="records", date_format="iso"))
        cleaned_json_rows: List[Dict] = json.loads(cleaned_df.to_json(orient="records", date_format="iso")) # convert df to dict to avoid missinterpretation of NaN values
        for idx, (raw_row, cleaned_row) in enumerate(zip(raw_json_rows, cleaned_json_rows)):
            out: Dict = {}


            for col, val in raw_row.items():
               out[f"{col}.raw"] = val
            for col, val in cleaned_row.items():
                out[f"{col}.cleaned"] = val
            
            if cleaned_row.get(self._timestamp_attribute):
                out["ts"] = cleaned_row[self._timestamp_attribute]

            self._publish(self._result_topic, out)
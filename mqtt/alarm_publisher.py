"""
AlarmPublisher
==============

Convert *alarm events* produced by CorrectionEngine into outbound messages
(typically MQTT, but any callable may be injected).

The alarm message format is **decoupled** from the underlying transport:
    • `publish(topic, obj_dict)` handles serialisation.
    • Row‑specific details (sensor index, unexpected value, etc.) are taken
      directly from the expectation‑result block Great‑Expectations returns.
"""
from __future__ import annotations

from typing import Callable, Dict, List
import datetime as _dt
import math
import pandas as pd


def _json_safe(value):
        """Convert pandas/NumPy nulls to a real JSON null (=Python None)."""
        if value is None:
            return None
        # True for numpy.nan, pandas.NA, pd.NaT …  
        if pd.isna(value) or (isinstance(value, float) and math.isnan(value)):
            return None
        return value


class AlarmPublisher:
    """Tiny façade that owns *only* alarm‑formatting rules."""

    def __init__(
        self,
        topic_cfg: Dict,                       # e.g. cfg.mqtt()["topics"][topic]
        publish: Callable[[str, dict], None],  # injected network layer
    ) -> None:
        self._alarm_topic: str = topic_cfg["publish"]["alarm"]
        self._publish = publish

    # ------------------------------------------------------------------ #
    #  public API
    # ------------------------------------------------------------------ #
    def emit(
        self,
        cleaned_df: pd.DataFrame,
        expectation_result: Dict
    ) -> None:
        """
        Emit one alarm per unexpected index using the `dateTo` timestamp.

        Parameters
        ----------
        column
            Column where the expectation failed.
        expectation_result
            One result entry from validation_results["results"].
        cleaned_df
            Cleaned DataFrame (to extract `dateTo` timestamps).
        """
        idx_list: List[int] = expectation_result["result"]["unexpected_index_list"]
        exp_type: str = expectation_result["expectation_config"]["type"]

        for row_idx in idx_list:
            try:
                ts = cleaned_df.loc[row_idx, "dateTo"]
                # if pd.isna(ts):
                #     continue  # Skip NaT/null timestamps
                # # Format to ISO8601 with timezone (if not already present)
                # ts_str = pd.to_datetime(ts).isoformat()
                # if not ts_str.endswith("Z") and "+" not in ts_str:
                #     ts_str += "+00:00"
                alarm_payload = {
                    "ts": ts,
                    "type": exp_type,
                    "severity": "CRITICAL"
                }
                self._publish(self._alarm_topic, alarm_payload)
            except Exception as e:
                print(f"⚠️  Failed to emit alarm for index {row_idx}: {e}")

    
    
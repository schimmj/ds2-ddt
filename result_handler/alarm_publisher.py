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
        column: str,
        expectation_result: Dict,
        raw_df            # pd.DataFrame  (type hint avoided to keep deps tiny)
    ) -> None:
        """
        Parameters
        ----------
        column
            Column name where the expectation failed.
        expectation_result
            One entry from `validation_results["results"]`.
        raw_df
            Original uncorrected DataFrame (used only to fetch raw value).
        """
        idx_list: List[int] = expectation_result["result"]["unexpected_index_list"]
        val_list           = expectation_result["result"]["unexpected_list"]
        exp_type           = expectation_result["expectation_config"]["type"]

        for row_idx, bad_value in zip(idx_list, val_list):
            bad_value = _json_safe(bad_value)
            alarm_payload = {
                "timestamp": _dt.datetime.utcnow().isoformat() + "Z",
                "column": column,
                "row_index": row_idx,
                "offending_value": bad_value,
                "expectation": exp_type,
                "severity": "CRITICAL",
                "message": (
                    f"{exp_type} failed at row {row_idx}: "
                    f"{column}={bad_value!r}"
                ),
            }
            self._publish(self._alarm_topic, alarm_payload)

    
    
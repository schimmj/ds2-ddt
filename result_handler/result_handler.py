"""
ResultHandler
=============

Coordinates three independent concerns:

1. **CorrectionEngine**  –– turns validation results + original DataFrame
   into a cleaned DataFrame **and** a list of alarm‑worthy failures.

2. **AlarmPublisher**    –– converts each alarm event into an outbound
   message via the *shared* publish‑callable.

3. **ResultPublisher**   –– row‑wise flattens the cleaned data and publishes
   it (also via the shared publish‑callable).

The network layer is injected once, globally, via
`ResultHandler.set_default_publisher()`.  BatchPipeline therefore needs no
knowledge of MQTT.
"""
from __future__ import annotations

from typing import Callable, List, Tuple

import pandas as pd

from data_correction import CorrectionEngine
from .alarm_publisher import AlarmPublisher
from .result_publisher  import ResultPublisher
from config import ConfigProvider
from data_correction import DataCorrection


class ResultHandler:
    """Orchestrates correction + publishing; owns no I/O of its own."""

    _default_publish: Callable[[str, dict], None] | None = None   # class‑level

    # --------------------------------------------------------------------- #
    #  class helpers
    # --------------------------------------------------------------------- #
    @classmethod
    def set_default_publisher(cls, fn: Callable[[str, dict], None]) -> None:
        """Register a process‑wide publish‑callable (e.g. MqttPublisher.publish)."""
        cls._default_publish = fn



    def __init__(self, topic: str, config_name:str, cfg: ConfigProvider | None = None, publish: Callable[[str, dict], None] | None = None) -> None:
        """
        Parameters
        ----------
        topic
            Logical sensor/topic name (must exist in both mqtt + validation configs).
        cfg
            Optional ConfigProvider instance.  If omitted, a default provider
            is created – it caches JSON so the cost is negligible.
        publish
            Optional call‑back `(mqtt_topic: str, obj: dict) -> None`.
            If *None*, the class‑level default set via `set_default_publisher`
            will be used.
        """
        cfg = cfg or ConfigProvider()

        # network layer ---------------------------------------------------- #
        publish = publish or ResultHandler._default_publish
        if publish is None:
            raise RuntimeError(
                "ResultHandler needs a publish‑callable, but none was provided "
                "and no default is registered.  "
                "Call ResultHandler.set_default_publisher() in main.py."
            )

        # domain engines --------------------------------------------------- #
        topic_cfg = cfg.mqtt()["topics"][topic]

        self._engine  = CorrectionEngine(topic, config_name, cfg, DataCorrection())
        self._alarms  = AlarmPublisher(topic_cfg, publish)
        self._results = ResultPublisher(topic_cfg, publish)

    # --------------------------------------------------------------------- #
    #  public API
    # --------------------------------------------------------------------- #
    def handle(
        self,
        validation_results: dict,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Apply corrections, raise alarms, publish cleaned data.

        Returns
        -------
        pd.DataFrame
            The cleaned DataFrame (also returned by `CorrectionEngine`).
        """
        cleaned_df, alarm_events = self._engine.run(validation_results, df)

        # --- alarms first ------------------------------------------------- #
        for alarm in alarm_events:
            self._alarms.emit(cleaned_df, alarm)

        # --- publish cleaned rows ---------------------------------------- #
        self._results.emit(cleaned_df, df)
        return cleaned_df

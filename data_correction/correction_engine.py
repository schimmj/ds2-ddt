# SPDX-FileCopyrightText: 2025 - 2025 Software GmbH, Darmstadt, Germany and/or its subsidiaries and/or its affiliates
# SPDX-License-Identifier: Apache-2.0

"""
CorrectionEngine
================

Transforms a `(validation_results, original_df)` pair into

    • cleaned_df      – a copy of the original with in‑place corrections
    • alarm_events    – list[tuple[column_name, result_dict]]

The engine is *pure* domain logic: it knows nothing about MQTT,
publishers, or sleeps.
"""
from __future__ import annotations

from ast import Raise
from typing import List, Tuple

import pandas as pd

from data_correction import DataCorrection
from config import ConfigProvider
from data_correction import is_valid_strategy


class CorrectionEngine:
    """Applies per‑column correction strategies; flags unfixable rows."""

    def __init__(
        self,
        topic: str,
        config_name: str,
        cfg: ConfigProvider,
        corrector: DataCorrection
    ) -> None:
        config_id = config_name.removesuffix("_"+topic)
        self._rules = cfg.validation()[config_id][topic]
        self._corrector = corrector

    # ------------------------------------------------------------------ #
    #  core logic
    # ------------------------------------------------------------------ #
    def run(
        self,
        validation_results: dict,
        df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, List[dict]]:
        """
        Returns
        -------
        cleaned_df : pd.DataFrame
        alarm_events : list[(column_name, expectation_result_dict)]
        """
        cleaned_df   = df.copy()
        alarm_events = []

        prev_column, exp_idx = None, 0

        for res in validation_results["results"]:
            col = res["expectation_config"]["kwargs"]["column"]

            # Track “position” when multiple expectations target same column
            if col == prev_column:
                exp_idx += 1
            else:
                exp_idx, prev_column = 0, col

            if res["success"]:
                continue  # nothing to do

            strategy = self._rules[col][exp_idx].get("handler")
            unexpected_idx = res["result"]["unexpected_index_list"]

            if strategy and is_valid_strategy(strategy):
                cleaned_df[col] = self._corrector.correct_column(
                    cleaned_df[col],
                    rows_to_correct=unexpected_idx,
                    strategy_name=strategy
                )
            elif strategy == "RaiseAlarm":
                alarm_events.append(res)  # is it necessary to put the whole result to an alarm?

        return cleaned_df, alarm_events

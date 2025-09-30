# SPDX-FileCopyrightText: 2025 - 2025 Software GmbH, Darmstadt, Germany and/or its subsidiaries and/or its affiliates
# SPDX-License-Identifier: Apache-2.0

# data_correction.py

from numpy import indices
import pandas as pd
from .correction_strategies import MissingValueImputation, SmoothingOutliers, CorrectionStrategy, get_strategy

class DataCorrection:
    def __init__(self):
        """
        Initialize with a dictionary mapping expectation types to correction strategies.
        """
   

    def correct_column(
        self, 
        column: pd.Series, 
        rows_to_correct: dict, 
        strategy_name: str, 
        min=None, 
        max=None
    ) -> pd.Series:
        """
        Correct a single column based on the given expectation result and the strategy to use for correction.
        If the correction result is a number, enforce min/max bounds.

        Parameters
        ----------
        column : pd.Series
            The column to correct.
        rows_to_correct : dict
            Indices that require correction.
        strategy_name : str
            Name of the correction strategy.
        min : float | int | None
            Lower bound for numeric corrections.
        max : float | int | None
            Upper bound for numeric corrections.
        """
        strategy_cls: type[CorrectionStrategy] = get_strategy(strategy_name)
        strategy = strategy_cls()
        corrected_column = column.copy()

        for index in rows_to_correct:
            value = strategy.apply(index=index, neighbours=column)

            # Clip numeric values if min/max are provided
            if isinstance(value, (int, float)):
                if min is not None and value < min:
                    value = min
                if max is not None and value > max:
                    value = max

            corrected_column[index] = value

        return corrected_column

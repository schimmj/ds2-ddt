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
   

    def correct_column(self, column: pd.Series, rows_to_correct:dict, strategy_name:str):
        """
        Correct a single column based on the given expectation result and the strategy to use for correction.
        
        """

        strategy_cls: type[CorrectionStrategy] = get_strategy(strategy_name)
        strategy = strategy_cls()
        corrected_column = column.copy()
        for index in rows_to_correct:
            corrected_column[index] = strategy.apply(index=index, neighbours=column)

        return corrected_column

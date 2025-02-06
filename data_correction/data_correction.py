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

        strategy: CorrectionStrategy = get_strategy(strategy_name)
        corrected_column = column.copy()
        for index in rows_to_correct:
            corrected_column[index] = strategy.apply(self, index=index, neighbours=column)

        return corrected_column

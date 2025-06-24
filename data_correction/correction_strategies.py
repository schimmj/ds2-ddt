# SPDX-FileCopyrightText: 2025 - 2025 Software GmbH, Darmstadt, Germany and/or its subsidiaries and/or its affiliates
# SPDX-License-Identifier: Apache-2.0

# correction_strategies.py

from enum import Enum
import time
import pandas
from dateutil.parser import parse
from datetime import datetime, timezone
import ast


class CorrectionStrategy:
    """Base class for correction strategies."""
    def apply(self, index, neighbours):
        raise NotImplementedError("Subclasses must implement the apply method.")
    
class CorrectionStrategyEnum(Enum):
    MissingValueImputation = "MissingValueImputation"
    SmoothingOutliers = "SmoothingOutliers"
    TimestampCorrection = "TimestampCorrection"
    

    def get_strategy_class(self) -> type[CorrectionStrategy]:
        # This method maps the enum name to the corresponding class
        if self == CorrectionStrategyEnum.MissingValueImputation:
            return MissingValueImputation
        elif self == CorrectionStrategyEnum.SmoothingOutliers:
            return SmoothingOutliers
        elif self == CorrectionStrategyEnum.TimestampCorrection:
            return TimestampCorrection
        else:
            raise ValueError(f"Unknown strategy: {self.name}")
        
        

class MissingValueImputation(CorrectionStrategy):
    def apply(self, index, neighbours):
        return index if index is not None else None

class SmoothingOutliers(CorrectionStrategy):
    """Smooths outliers to an average of its neighbors."""
    
    def apply(self, index, neighbours: pandas.Series):

        left_neighbours = neighbours.iloc[max(0, index-3):index].tolist()
        right_neighbours = neighbours.iloc[index+1:min(len(neighbours), index+4)].tolist()
        
        if len(left_neighbours) < 3:
            left_neighbours = [neighbours.iloc[0]] * (3 - len(left_neighbours)) + left_neighbours
        if len(right_neighbours) < 3:
            right_neighbours = right_neighbours + [neighbours.iloc[-1]] * (3 - len(right_neighbours))

        total_neighbors = left_neighbours + right_neighbours

        return sum(total_neighbors) / len(total_neighbors)
    
class TimestampCorrection(CorrectionStrategy):
    """
    Corrects various timestamp formats to a standardized ISO 8601 string.
    
    Handles:
    - Most common date-time strings (e.g., "2025-01-01T00:06:19.573").
    - String representations of lists (e.g., "'[ 2023, 1, 2, 2, 0 ]'").
    - Actual lists or tuples (e.g., [2023, 1, 2, 2, 0]).
    """
    def apply(self, index, neighbours: pandas.Series):
        """
        Applies the timestamp correction.
        
        :param value: The timestamp value to correct (string, list, etc.).
        :param neighbours: Unused for this strategy, kept for compatibility.
        :return: A timestamp string in ISO 8601 format.
        """
        value = neighbours.iloc[index]


        if value is None:
            return None
        
        try:
            # Handle if the value is a string representation of a list
            # e.g., "'[2023, 1, 2, 2, 0]'"
            if isinstance(value, str) and value.strip().startswith('['):
                 # Safely evaluate the string to a list
                components = ast.literal_eval(value)
                dt_obj = datetime(*components)
            # Handle if the value is already a list or tuple
            elif isinstance(value, (list, tuple)):
                dt_obj = datetime(*value)
            # Otherwise, use the powerful dateutil parser for strings
            else:
                dt_obj = parse(str(value))
                
            if dt_obj.tzinfo is None:
                # If no timezone info, assume UTC
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            return dt_obj.isoformat()  # Return in ISO 8601 format
            

        except (ValueError, TypeError, SyntaxError) as e:
            # Handle cases where parsing fails
            print(f"Could not parse timestamp '{value}': {e}")
            return None # Or return the original value, or a default


def is_valid_strategy(strategy_name):
    """Check if a strategy name exists in the CorrectionStrategyEnum."""
    return strategy_name in CorrectionStrategyEnum._member_names_
  




def get_strategy(strategy_name):
    """
    Retrieve the strategy class corresponding to the given strategy name.
    :param strategy_name: Name of the correction strategy as a string.
    :return: CorrectionStrategy subclass if the name is valid, else raise ValueError.
    """
    try:
        # Convert strategy name to Enum and return the corresponding class
        strategy_enum = CorrectionStrategyEnum[strategy_name]
        return strategy_enum.get_strategy_class()
    except KeyError:
        raise ValueError(f"Invalid strategy name: {strategy_name}")

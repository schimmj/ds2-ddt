# correction_strategies.py

from enum import Enum
import pandas


class CorrectionStrategy:
    """Base class for correction strategies."""
    def apply(self, index, neighbours):
        raise NotImplementedError("Subclasses must implement the apply method.")
    
class CorrectionStrategyEnum(Enum):
    MissingValueImputation = "MissingValueImputation"
    SmoothingOutliers = "SmoothingOutliers"
    

    def get_strategy_class(self) -> CorrectionStrategy:
        # This method maps the enum name to the corresponding class
        if self == CorrectionStrategyEnum.MissingValueImputation:
            return MissingValueImputation
        elif self == CorrectionStrategyEnum.SmoothingOutliers:
            return SmoothingOutliers
        else:
            raise ValueError(f"Unknown strategy: {self.name}")
        
        

class MissingValueImputation(CorrectionStrategy):
    """Handles missing values by replacing them with a default."""
    def __init__(self, default_value):
        self.default_value = default_value

    def apply(self, index, neighbours):
        return index if index is not None else self.default_value

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

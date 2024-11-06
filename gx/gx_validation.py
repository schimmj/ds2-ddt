import great_expectations as gx
import pandas as pd
from datetime import datetime

from urllib3 import Retry

# Great Expectations setup
context = gx.get_context()  # Or use EphemeralDataContext if you don't want to use great_expectations.yml



data_source_name = "my_data_source"
data_source = context.data_sources.add_pandas(name=data_source_name)

data_asset_name = "my_dataframe_data_asset"
data_asset = data_source.add_dataframe_asset(name=data_asset_name)

batch_definition_name = "my_batch_definition"


batch_definition = data_asset.add_batch_definition_whole_dataframe(
    batch_definition_name
)








def validate_batch(df):
    

    batch_parameters = {"dataframe": df}
    
    expectation = gx.expectations.ExpectColumnValuesToBeBetween(column="tavg", max_value=40, min_value=-5)

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)
    
    validation_results = batch.validate(expectation)
    
    print(validation_results)
    
    return validation_results
    

    
    
    
    
    
    

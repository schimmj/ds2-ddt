import great_expectations as gx
import pandas as pd
from datetime import datetime

from urllib3 import Retry

# Great Expectations setup
context = gx.get_context()  # Or use EphemeralDataContext if you don't want to use great_expectations.yml


#Creating a data source
data_source_name = "mqtt-data-source"
data_source = context. data_sources.add_pandas(name=data_source_name)

#Creating a data Asset
data_asset_name = "mqtt-data-asset"
data_asset = data_source.add_dataframe_asset(name=data_asset_name)


# Define the Batch Definition
batch_definition_name = "mqtt-batch-definition"
batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

# Creating an Expectation Suite
suite_name_1 = "weather_expectations"
suite_weather = gx.ExpectationSuite(name=suite_name_1)
context.suites.add(suite_weather)



# Creating an Expectation Suite
suite_name_2 = "traffic_expectations"
suite_traffic = gx.ExpectationSuite(name=suite_name_2)
context.suites.add(suite_traffic)


expectation_2 = gx.expectations.ExpectColumnValuesToBeBetween(column="tavg", max_value=40, min_value=-10)
suite_weather.add_expectation(expectation_2)

expectation_1 = gx.expectations.ExpectColumnValuesToBeBetween(column="tavg", max_value=20, min_value=-5)
suite_traffic.add_expectation(expectation_1)








def validate_batch(df: pd.DataFrame, topic):
    batch_parameters = {"dataframe": df}
    
    # Creating a Validation Definition
    definition_name = f"{topic}_definition"
    
    try:
        validation_definition = context.validation_definitions.get(definition_name)
    except Exception: 
        if topic == "your/traffic":
            validation_definition = gx.ValidationDefinition(data=batch_definition, suite=suite_traffic, name=definition_name)
        else:
            validation_definition = gx.ValidationDefinition(data=batch_definition, suite=suite_weather, name=definition_name)
        validation_definition = context.validation_definitions.add(validation_definition)
    
    
    
    
    # Run the validation definition with the batch parameters
    validation_result = validation_definition.run(batch_parameters=batch_parameters)
    
    print(validation_result)
    
    return validation_result

    

    
    
    
    
    
    

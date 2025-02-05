# %%
# Import required modules from GX library.
import json
import great_expectations as gx

import pandas as pd

# Create Data Context.
context = gx.get_context(mode='file')


#%%
#Creating a data source, data asset and batch definition
data_source_name = "pandas-data-source"
data_source = context. data_sources.add_pandas(name=data_source_name)

data_asset_name = "mqtt-data-asset"
data_asset = data_source.add_dataframe_asset(name=data_asset_name)

batch_definition_name = "mqtt-batch"
batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)




# %%
# Import sample data into Pandas DataFrame.
data_path = "./../demo_data/weather_january.json"
with open(data_path, 'r') as file:
    df = pd.read_json(file)
    
metadata_df = pd.DataFrame(df['metdata'].tolist())
batch_parameters = {"dataframe": metadata_df}


# %%
# Creating an Expectation Suite and adding it to the data context
suite_name = "weather_expectations"
suite = gx.ExpectationSuite(name=suite_name)
suite = context.suites.add(suite)

#%% 
# Creating an Expectation and adding it to the suite
temperature_expectation = gx.expectations.ExpectColumnStdevToBeBetween(column='tavg', min_value=-5, max_value=20)
suite.add_expectation(temperature_expectation)



#%% 
# Creating a Validation Definition
definition_name = "weather_validation_definition"
validation_definition = gx.ValidationDefinition(data=batch_definition, suite=suite, name=definition_name)
validation_definition = context.validation_definitions.add(validation_definition)




#%% 
# Run the validation and print it
validation_result = validation_definition.run(batch_parameters=batch_parameters)
print(validation_result)

# %%

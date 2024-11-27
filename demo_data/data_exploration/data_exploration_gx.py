# %%
# Import required modules from GX library.
import json
import great_expectations as gx

import pandas as pd

# Create Data Context.
context = gx.get_context()

# %%
# Import sample data into Pandas DataFrame.
data_path = "./../weather_january.json"
with open(data_path, 'r') as file:
    df = pd.read_json(file)
    
    
# %%
metadata_df = pd.DataFrame(df['metdata'].tolist())

#%%
#Creating a data source
data_source_name = "mqtt-data-source"
data_source = context. data_sources.add_pandas(name=data_source_name)


#%%
#Creating a data Asset
data_asset_name = "mqtt-data-asset"
data_asset = data_source.add_dataframe_asset(name=data_asset_name)


#%%
# Define the Batch Definition
batch_definition_name = "mqtt-exploration"
batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)



#%%
# Storing the dataframe in batch parameter dictionary
batch_parameters = {"dataframe": metadata_df}




# %%
# Creating an Expectation Suite and adding it to the data context
suite_name = "weather_expectations"
suite = gx.ExpectationSuite(name=suite_name)
suite = context.suites.add(suite)


#%%
# Checking if the suite is still fresh
print(suite.is_fresh())




#%% 
# Creating an Expectation and adding it to the suite

temperature_expectation_2 = gx.expectations.ExpectColumnStdevToBeBetween(column='tavg', min_value=0, max_value=7)
suite.add_expectation(temperature_expectation_2)


#%%
# Creating another expectation
temperature_expectation = gx.expectations.ExpectColumnValuesToBeBetween(column="tavg", min_value=-5, max_value=40)
suite.add_expectation(temperature_expectation)



#%% 
# Creating a Validation Definition
definition_name = "my_validation_definition"
validation_definition = gx.ValidationDefinition(data=batch_definition, suite=suite, name=definition_name)
validation_definition = context.validation_definitions.add(validation_definition)


#%%
# Checking if validation_definition is still fresh
validation_definition.is_fresh()



#%% 
# Run the validation and print it
validation_result = validation_definition.run(batch_parameters=batch_parameters)
print(validation_result)



# %%
# Save the expectation suite as a json
expectation_suite = suite.to_json_dict()
print(expectation_suite)



# %%
# Test to load an expectation suite
jsonschema_file = "./expectations/weather_expectations.json"
suite_name = "weather_expectation"

with open(jsonschema_file, "r") as f:
  schema = json.load(f)
  
profiler = JsonSchemaProfiler() # type: ignore

suite = profiler.profile(schema, suite_name)

context.add_expectation_suite(expectation_suite=suite)
# %%

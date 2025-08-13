# %%
# Import required modules from GX library.
import json
import validation as gx

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
data_path = "./../demo_data/ARSO_air_quality_hourly_with_outliers_test.json"
with open(data_path, 'r') as file:
    df = pd.read_json(file)
    

batch_parameters = {"dataframe": df}


# %%
# Creating an Expectation Suite and adding it to the data context
suite_name = "air_quality_expectations"
suite = gx.ExpectationSuite(name=suite_name)
suite = context.suites.add(suite)
suites = context.suites

#%% 
# Creating an Expectation and adding it to the suite
# temperature_expectation = gx.expectations.ExpectColumnValuesToBeBetween(column='tavg', min_value='-2', max_value='20')
timestamp_expectation = gx.expectations.ExpectColumnValuesToMatchRegex(column='dateTo', regex=r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}(:?\d{2})?)$')
# hints = gx.expectations.ExpectColumnValuesToBeBetween.__annotations__
# print(hints)
suite.add_expectation(timestamp_expectation)
# help(gx.expectations.ExpectColumnStdevToBeBetween)



#%% 
# Creating a Validation Definition
definition_name = "air_quality_validation_definition"
validation_definition = gx.ValidationDefinition(data=batch_definition, suite=suite, name=definition_name)






#%% 
# Run the validation and print it
validation_result = validation_definition.run(batch_parameters=batch_parameters)
print(validation_result)

# %%

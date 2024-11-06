# %%
# Import required modules from GX library.
import great_expectations as gx

import pandas as pd

# Create Data Context.
context = gx.get_context()

# %%
# Import sample data into Pandas DataFrame.
data_path = "weather_january.json"
with open(data_path, 'r') as file:
    df = pd.read_json(file)
    
    
# %%
metadata_df = pd.DataFrame(df['metdata'].tolist())

#%%
# Connect to data.
# Create Data Source, Data Asset, Batch Definition, and Batch.
data_source = context.data_sources.add_pandas("pandas")
data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")

batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
batch = batch_definition.get_batch(batch_parameters={"dataframe": metadata_df})

# %%
# Create Expectation suite
suite_name = "my_expectation_suite"
suite = gx.ExpectationSuite(name=suite_name)
suite = context.suites.add(suite)


# %%
# Create and add expectations ot the suite
expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    column="passenger_count", min_value=1, max_value=6
)

suite.add_expectation(expectation)

#%%
# Create a validation definition and
definition_name = "my_validation_definition"
validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=suite, name=definition_name
)
validation_definition = context.validation_definitions.add(validation_definition)



# %%
validation_definition.run()
# %%

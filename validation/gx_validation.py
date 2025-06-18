import great_expectations as gx
import pandas as pd
from utils.utils import topic_url_to_name
from datetime import datetime


from urllib3 import Retry

# Great Expectations setup
context = gx.get_context(mode='file', project_root_dir='./validation')  # Or use EphemeralDataContext if you don't want to use great_expectations.yml


def validate_batch(df: pd.DataFrame, topic):
    batch_parameters = {"dataframe": df}
    
    # Creating a Validation Definition
    definition_name = f"{topic_url_to_name(topic)}_validation_definition"
    
    try:
        validation_definition = context.validation_definitions.get(definition_name)
    except Exception: 
        print(f'Error: {definition_name} does not exist!')
        
        
    # Run the validation definition with the batch parameters
    validation_result = validation_definition.run(batch_parameters=batch_parameters, result_format="COMPLETE")
    
    return validation_result

    

    
    
    
    
    
    

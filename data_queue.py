from inspect import cleandoc
import pandas as pd
from gx.gx_validation import validate_batch
import json

ALARM_TOPIC = "your/alarm"
VALIDATION_RESULT_TOPIC = "your/validation"

BATCH_SIZE = 10 # Queue size for batch processing


data_queue = []

def add_to_queue(data, client):
    """Add data to the queue."""
    global data_queue

    data_queue.append(data)
    # print(f"Queue length {len(data_queue)}")
    if len(data_queue) >= BATCH_SIZE:
        process_batch(client)
        

def process_batch(client):
    """Process and validate a batch of data."""
    global data_queue
    if len(data_queue) == 0:
        publish_alarm("Warning: Sensor is not sending anymore!", client)
    else: 
        df = pd.DataFrame(data_queue)
        validation_results = validate_batch(df)  # Pass the batch to Great Expectations for validation
        # Publish the validation results to MQTT
        publish_results(validation_results, client)
    
    data_queue = []  # Clear the queue
    
    
    

def publish_results(results, client):
    """Publish the validation results."""
    result_json = json.dumps(results, default=str)  # Convert results to JSON string
    message_info = client.publish(VALIDATION_RESULT_TOPIC, result_json)
    publised = message_info.is_published()
    print(f"Result published: {publised}")
    
    
def publish_alarm(alarm, client):
    """Publish the a raised alarm to MQTT."""
    result_json = json.dumps(alarm, default=str)  # Convert results to JSON string
    
    try:
        message_info = client.publish(ALARM_TOPIC, result_json)
        message_info.wait_for_publish(5)
        print(f"Alarm published on topic \"{ALARM_TOPIC}\": {alarm}\n." )
    except:
        print(f"Alarm was not published successfully! \n")

        
    

    
    



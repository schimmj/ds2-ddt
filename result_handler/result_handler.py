# result_handler.py

from logging import config
from xml.sax import handler
import pandas as pd
import json
import os
import time
from dotenv import load_dotenv
from config import ConfigLoader
from data_correction import DataCorrection, SmoothingOutliers, MissingValueImputation, is_valid_strategy

load_dotenv()

# Configuration via environment variables
BROKER = os.getenv("BROKER", "localhost")  # Address of the MQTT broker
PORT = int(os.getenv("PORT", 1883))        # Port to connect to the MQTT broker

class ResultHandler:
    """
    Handles validation results by applying corrections or raising alarms
    based on a configurable set of strategies.
    """
    def __init__(self, topic: str):
        """
        Initialize the ResultHandler.

        :param config_path: Path to the JSON configuration file.
        :param mqtt_config_path: Path to the MQTT configuration JSON file.
        """
        load_dotenv()        
        self.mqtt_config = ConfigLoader().load_config("mqtt_config.json")
        self.validation_config = ConfigLoader().load_config("validation_config.json")
        self.corrector = DataCorrection()
        self.topic = topic
        
        

    def handle_results(self, validation_results: dict, data_batch: pd.DataFrame) -> pd.DataFrame:
        """
        Process validation results and apply corrections or alarms.

        :param validation_results: GX validation results as a dictionary.
        :param data_batch: The original DataFrame to be corrected.
        :return: A corrected DataFrame.
        """
        corrected_data = data_batch.copy()
        expectation_position = 0
        prev_column = None

        for result in validation_results["results"]:
            column = result["expectation_config"]["kwargs"]["column"]
            if prev_column == column:
                expectation_position += 1
            else:
                expectation_position = 0
            prev_column = column
            
            if not result['success']:
                handling_strategy = self.validation_config["validations"][self.topic][column][expectation_position]['handler']
                print(f"Result handler: {handling_strategy}")
                if is_valid_strategy(handling_strategy):
                    corrected_data[column] = self.corrector.correct_column(
                        column=corrected_data[column],
                        rows_to_correct=result['result']['unexpected_index_list'],
                        strategy_name=handling_strategy
                    )
                else:
                    self.raise_alarm_per_row(column, result, data_batch)
                    
        from mqtt.mqtt_handler import MQTTHandler
        sender = MQTTHandler(BROKER, PORT)     
        sender.publish_results(self.topic, corrected_data) 
        return corrected_data
    
    
    

    

    def raise_alarm_per_row(self, column: str, result, data_batch: pd.DataFrame):
        """
        Raise an alarm for a specific column and expectation type.
        """
        from mqtt.mqtt_handler import MQTTHandler
        alarmer = MQTTHandler(BROKER, PORT) 
        expectation_type = result["expectation_config"]["type"]
        unexpected_index_list = result["result"]["unexpected_index_list"]
        unexpected_values = result["result"]["unexpected_list"]
        
        for index, value in zip(unexpected_index_list, unexpected_values):
            time = data_batch.iloc[index]["time"]
            alarm_message = f"An {expectation_type} expectation occured at {time}: Attribute: {column}, Value: {value}"
            alarmer.publish_alarm(initial_topic=self.topic, message=alarm_message)
        
        
        
        
        


# result_handler.py

from logging import config
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
    def __init__(self):
        """
        Initialize the ResultHandler.

        :param config_path: Path to the JSON configuration file.
        :param mqtt_config_path: Path to the MQTT configuration JSON file.
        """
        load_dotenv()        
        self.mqtt_config = ConfigLoader().load_config("mqtt_config.json")
        self.validation_config = ConfigLoader().load_config("validation_config.json")
        self.corrector = DataCorrection()
        
        

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
            result_handler = self.validation_config["validations"][column][expectation_position]
            print(f"Result handler: {result_handler}")
        return corrected_data

    def raise_alarm(self, column: str, expectation_type: str, time):
        """
        Raise an alarm for a specific column and expectation type.
        """
        from mqtt.mqtt_handler import MQTTHandler
        alarmer = MQTTHandler(BROKER, PORT, topic_handlers=None) 
        alarmer.publish_alarm(alarm=f"ALARM: {expectation_type} raised for column '{column}' at time {time}", alarm_topic= ALARM_TOPIC_WEATHER)


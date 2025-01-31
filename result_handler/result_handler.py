# result_handler.py

from logging import config
import pandas as pd
import json
import os
import time
from dotenv import load_dotenv

from data_correction import DataCorrection, SmoothingOutliers, MissingValueImputation, is_valid_strategy

load_dotenv()

# Configuration via environment variables
BROKER = os.getenv("BROKER", "localhost")  # Address of the MQTT broker
PORT = int(os.getenv("PORT", 1883))        # Port to connect to the MQTT broker
PUBLISH_TOPIC_WEATHER =os.getenv("WEATHER_TOPIC_VALIDATED", "weather/validated")
ALARM_TOPIC_WEATHER = os.getenv("WEATHER_TOPIC_ALARM")

class ResultHandler:
    """
    Handles validation results by applying corrections or raising alarms
    based on a configurable set of strategies.
    """

    def __init__(self, config_path: str = "result_handler/config.json"):
        """
        Initialize the ResultHandler.

        :param config_path: Path to the JSON configuration file.
        """
        load_dotenv()
        with open(config_path, 'r') as config_file:
            self.config: dict = json.load(config_file)
        self.corrector = DataCorrection()

    def handle_results(self, validation_results: dict, data_batch: pd.DataFrame) -> pd.DataFrame:
        """
        Process validation results and apply corrections or alarms.

        :param validation_results: GX validation results as a dictionary.
        :param data_batch: The original DataFrame to be corrected.
        :return: A corrected DataFrame.
        """
        corrected_data = data_batch.copy()

        for result in validation_results["results"]:
            column = result["expectation_config"]["kwargs"]["column"]
            expectation_type = result["expectation_config"]["type"]

            handle_strategy = self.config.get(column, {}).get(expectation_type)

            if is_valid_strategy(handle_strategy):
                corrected_data[column] = self.corrector.correct_column(
                    data_batch[column], expectation_type, result, handle_strategy
                )
                invalid_indices = result["result"]["partial_unexpected_index_list"]
                for i in invalid_indices:
                    self.raise_alarm(column, expectation_type, data_batch['time'][i] )
            elif handle_strategy == "RaiseAlarm":
                #Demo purpose comment
                # self.raise_alarm(column, expectation_type)
                continue
            else:
                print(f"No action configured for ({column}, {expectation_type})")
                
        from mqtt.mqtt_handler import MQTTHandler
        
        publisher = MQTTHandler(BROKER, PORT, topic_handlers=None)
        
                   
        for _, row in corrected_data.iterrows():
            formatted_row = {
                key: {
                    "raw": data_batch.at[row.name, key],
                    "cleaned": value
                } if key!= "time" else value for key, value in row.items()
            }
            # Publish each row
            publisher.publish_results(results=formatted_row, publish_topic=PUBLISH_TOPIC_WEATHER)
            time.sleep(0.1)
        print("Result published")

        return corrected_data

    def raise_alarm(self, column: str, expectation_type: str, time):
        """
        Raise an alarm for a specific column and expectation type.
        """
        from mqtt.mqtt_handler import MQTTHandler
        alarmer = MQTTHandler(BROKER, PORT, topic_handlers=None) 
        alarmer.publish_alarm(alarm=f"ALARM: {expectation_type} raised for column '{column}' at time {time}", alarm_topic= ALARM_TOPIC_WEATHER)


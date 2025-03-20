import pandas as pd
import os
from dotenv import load_dotenv
from config import ConfigLoader
from data_correction import DataCorrection, is_valid_strategy

load_dotenv()

BROKER = os.getenv("BROKER", "localhost") 
PORT = int(os.getenv("PORT", 1883))       

class ResultHandler:
    """
    Processes validation results by applying corrections or raising alarms 
    based on configured strategies.
    """
    def __init__(self, topic: str):

        self.mqtt_config = ConfigLoader().load_config("generated_mqtt_config.json")
        self.validation_config = ConfigLoader().load_config("generated_validation_config.json")
        self.corrector = DataCorrection()
        self.topic = topic

    def handle_results(self, validation_results: dict, data_batch: pd.DataFrame) -> pd.DataFrame:
        """
        Process validation results:
         - Apply corrections for invalid data.
         - Raise alarms when necessary.
        Returns the corrected DataFrame.
        """
        corrected_data = data_batch.copy()
        expectation_position = 0  # Track multiple expectations for the same column.
        prev_column = None

        # Iterate over each validation result.
        for result in validation_results["results"]:
            column = result["expectation_config"]["kwargs"]["column"]
            if prev_column == column:
                expectation_position += 1
            else:
                expectation_position = 0
            prev_column = column

            if not result['success']:
                handling_strategy = self.validation_config["validations"][self.topic][column][expectation_position].get('handler')
                if handling_strategy is None:
                    continue  # Skip if no strategy
                else:
                    if is_valid_strategy(handling_strategy):
                        corrected_data[column] = self.corrector.correct_column(
                            column=corrected_data[column],
                            rows_to_correct=result['result']['unexpected_index_list'],
                            strategy_name=handling_strategy
                        )
                    else:
                        self.raise_alarm_per_row(column, result, data_batch)

        self.publish_result_per_row(corrected_data, data_batch)
        return corrected_data

    def publish_result_per_row(self, corrected_data: pd.DataFrame, data_batch: pd.DataFrame):
        """
        Publish each row's raw and corrected data via MQTT.
        """
        # Import MQTTHandler here to avoid circular dependencies.
        from mqtt.mqtt_handler import MQTTHandler
        sender = MQTTHandler(BROKER, PORT)
        for idx, row in corrected_data.iterrows():
            raw_row = data_batch.loc[idx]
            message = {}
            # Build a message with both raw and cleaned values
            for column in corrected_data.columns:
                message[column] = {
                    "raw": raw_row.get(column),
                    "cleaned": row.get(column)
                }
            validated_topic = self.mqtt_config['topics'][self.topic]["publish"]["validated"]
            sender.publish_results(validated_topic=validated_topic, results=message)

    def raise_alarm_per_row(self, column: str, result, data_batch: pd.DataFrame):
        """
        Raise an alarm for each invalid data point in a specific column.
        """
        from mqtt.mqtt_handler import MQTTHandler
        alarmer = MQTTHandler(BROKER, PORT)
        expectation_type = result["expectation_config"]["type"]
        unexpected_index_list = result["result"]["unexpected_index_list"]
        unexpected_values = result["result"]["unexpected_list"]

        for index, value in zip(unexpected_index_list, unexpected_values):
            raw_data = data_batch.loc[index]
            alarm_message = (
                f"An {expectation_type} expectation occurred at {raw_data}: "
                f"Attribute: {column}, Value: {value}"
            )
            alarm_topic = self.mqtt_config['topics'][self.topic]["publish"]["alarm"]
            alarmer.publish_alarm(alarm_topic=alarm_topic, message=alarm_message)

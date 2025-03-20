# MQTT Message Handler

This project is designed to handle MQTT messages and process them into topic-specific data queues for further handling. The MQTT broker connection is managed by the `MQTTHandler` class.

## Requirements

- Python 3.7+
- `paho-mqtt` library
- `great-expectations` library

## Installation

The whole installation were tested so far on a linux subsystem, but may work on windows as well. Keep in mind, that a potential integration of `thin-edge` may be more difficult.

1. Clone the repository:
    ```sh
    git clone https://github.com/SoftwareAG-RES/ds2_quality_module.git
    cd ds2_quality_module
    ```

2. If wanted create a virtual python environment:
    ```sh
    python3 -m venv venv
    source venv/bin/activate
    ```

3. Install the required packages:
    ```sh
    pip install -r requirements.txt
    ```

## Usage

1. Before running the main script, verify that both `generated_mqtt_config.json` and `generated_validation_config.json` exist the in `config`directory:
    ```sh
    python3 main.py
    ```

2. The script will start the MQTT client and wait for incoming messages.

3. To test the module, use the `data_sender.py` script to publish sample measurements to the MQTT broker.
    ```sh
    python3 data_sender.py mqtt/topic/to/publish/data
    ```

4. Module outputs can be monitored using the `data_reader.py`script.
    ```sh
    python3 data_reader.py mqtt/topic/validated
    python3 data_reader.py mqtt/topic/alarms
    ```

5. If you generated the configs yourself with the [E2C UI](https://github.com/azk_sagemu/ds2-e2c-ui) then make sure that your selected `mqtt/topics` align with the `generated_mqtt_config.json`. 

6. To stop the script, press `Ctrl+C`.

## Project Structure

- `main.py`: The main script to start the MQTT client and the validation initialization.
- `mqtt`: Contains the `MQTTHandler` class to manage the MQTT broker connection.
- `data_queue`: Defines the `DataQueue` class for handling topic-specific data queues.
- `validation`: Contains the `GXInitializer` class to initialize the the `great-expectations` rules. Furthermore it contains the actual execution batch validation.
- `result_handler`: Defines how possible data issues, detected by `great-expectations`, are handled.

## License

tbd
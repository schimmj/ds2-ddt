# MQTT Message Handler

A containerized FastAPI service that ingests measurements from MQTT or HTTP, queues them, and validates them against configurable data quality rules.

## Requirements

- Docker (Desktop or Engine) with Docker Compose v2 (docker compose command)
- (Optional) Postman or curl for HTTP testing

## Installation


1. Clone the repository:
    ```sh
    git clone https://github.com/SoftwareAG-RES/ds2_quality_module.git
    cd ds2_quality_module
    ```
2. Checkout the release version:
    ```
    git checkout v1.0.0
    ```

3. Build & run
    ```
    docker-compose up --build
    ```

## Usage


1. To test the modules mqtt pipeline, use the `data_sender.py` script to publish sample measurements to the MQTT broker.
    ```
    docker exec -it data-quality bash
    python3 data_sender.py air-quality -f demo_data/ARSO_air_quality_hourly_outliers.json
    ```

2. Use Postman or curl to call the API endpoints. Refer to [API Docs](http://localhost:8000/docs) for the exact paths and schemas.


## License

Apache-2.0
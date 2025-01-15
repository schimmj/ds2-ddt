# Use an official Ubuntu image as a base
FROM ubuntu:latest

# Install system tools, Thin Edge CLI, Python, and pip
RUN apt-get update && apt-get install -y \
    curl \
    mosquitto-clients \
    systemd \
    ca-certificates \
    python3 \
    python3-pip \
    && curl -fsSL https://thin-edge.io/install.sh | sh -s \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy and prepare Thin Edge configuration script
COPY configure_tedge.sh /app/configure_tedge.sh
RUN chmod +x /app/configure_tedge.sh

# Set the working directory in the container
WORKDIR /app

# Copy the Python requirements file
COPY requirements.txt /app/

# Install Python dependencies
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

# Copy the rest of the application code
COPY . /app/

# Expose the MQTT ports
EXPOSE 1883 8883

# Set default environment variables (can be overridden by docker-compose or command line)
# ENV BROKER mosquitto
# ENV PORT 1883
# ENV WEATHER_TOPIC weather
# ENV TRAFFIC_TOPIC traffic
# ENV WEATHER_TOPIC_VALIDATED weather/validated
# ENV RESULT_HANDLER_CONFIG result_handler/config.json

# Ensure the Python interpreter is specified explicitly
ENV PYTHONUNBUFFERED=1

# Keep the container running indefinitely if no command is specified
CMD ["bash", "/app/configure_tedge.sh"]

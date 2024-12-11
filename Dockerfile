# Use an official Python runtime as a parent image
FROM python:3.12

# Set environment variables to prevent Python from writing .pyc files and buffering output
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Copy only requirements file first (for leveraging Docker's caching mechanism)
COPY requirements.txt /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . /app/

# Expose the port used by the application (if any)
# Note: MQTT will use its own container for broker
EXPOSE 1883

# Set default environment variables (can be overridden by docker-compose or command line)
ENV BROKER mqtt-broker
ENV PORT 1883
ENV WEATHER_TOPIC weather
ENV TRAFFIC_TOPIC traffic
ENV WEATHER_TOPIC_VALIDATED weather/validated
ENV RESULT_HANDLER_CONFIG result_handler/config.json

# Command to run the application
CMD ["python", "main.py"]

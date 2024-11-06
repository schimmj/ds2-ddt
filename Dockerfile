FROM python:3.12
WORKDIR /app
COPY requirements.txt /app
COPY mqtt_listener.py /app
RUN pip install -r requirements.txt
CMD ["python", "mqtt_listener.py"]  

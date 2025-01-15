#!/bin/bash
set -e

# Stelle sicher, dass der Broker verfügbar ist
until mosquitto_pub -h mosquitto -t "test/topic" -m "Connection Test"; do
    echo "Warte auf Mosquitto..."
    sleep 2
done

echo "Thin Edge ist bereit und mit Mosquitto verbunden."

tedge config set mqtt.client.host mosquitto
tedge config set http.client.host tedge
tedge config set c8y.proxy.client.host tedge-mapper-c8y
tedge config set c8y.proxy.bind.address 0.0.0.0
tedge config set mqtt.bind.address 0.0.0.0
tedge config set http.bind.address 0.0.0.0
tedge config set c8y.url "$C8Y_URL"

tail -f /dev/null
import time
from mqtt_handler import start_mqtt_listener, start_mqtt_sender
from data_queue import BATCH_SIZE, process_batch




def main():
    
    # Start MQTT listener
    listener_client = start_mqtt_listener()
    sender_client = start_mqtt_sender()
    print("=" * 80 + "\n")
    print("MQTT client connect successfully.")
    print("=" * 80 + "\n")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Exiting...")
        listener_client.loop_stop()  # Stop the MQTT listener
        listener_client.disconnect()
        sender_client.loop_stop()
        sender_client.disconnect()
        


if __name__ == "__main__":
    main()

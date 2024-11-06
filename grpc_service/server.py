from concurrent import futures
import grpc
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import json
import your_proto_file_pb2
import your_proto_file_pb2_grpc
import threading

# Configuration for ThingSpeak's MQTT
MQTT_BROKER = "mqtt3.thingspeak.com"
MQTT_PORT = 1883
MQTT_TOPIC = "channels/920137/subscribe"
MQTT_USERNAME = "MTsiIiwfESscKwkUMDsZFwM"
MQTT_PASSWORD = "jtZVtGUMYZtvXcDsV/txwxc/"

# Configuration for Kafka (AWS MSK)
KAFKA_BROKER_URL = "b-1.democluster1.iv249r.c3.kafka.us-west-1.amazonaws.com:9092"  # Replace with your MSK cluster's broker URL
KAFKA_TOPIC = "DustSensorTopic"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER_URL],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# gRPC service implementation
class ThingSpeakServiceServicer(your_proto_file_pb2_grpc.ThingSpeakServiceServicer):
    def __init__(self):
        pass  # No gRPC subscriber logic needed since we are producing directly to Kafka

def on_message(client, userdata, msg):
    message = msg.payload.decode()
    # Produce message to Kafka topic
    producer.send(KAFKA_TOPIC, {'channel_id': '920137', 'field_data': message, 'timestamp': 'Timestamp here'})
    print(f"Produced message to Kafka topic '{KAFKA_TOPIC}': {message}")

def start_mqtt_client():
    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC)
    client.loop_start()

def serve():
    # MQTT client runs in a separate thread to keep the main thread free
    mqtt_thread = threading.Thread(target=start_mqtt_client)
    mqtt_thread.start()

    print("MQTT client is running and producing to Kafka...")
    try:
        mqtt_thread.join()  # Keep the main thread alive
    except KeyboardInterrupt:
        print("Shutting down...")
        mqtt_thread.join()

if __name__ == '__main__':
    serve()


from concurrent import futures
from paho.mqtt import client as mqtt_client
from kafka import KafkaProducer
import json
import threading
from creds import creds_dict

# Configuration for ThingSpeak's MQTT
MQTT_BROKER = creds_dict["BROKER"]
MQTT_PORT = creds_dict["PORT"] 
MQTT_TOPIC = creds_dict["TOPIC"] 
MQTT_CLIENT = creds_dict["CLIENT"]
MQTT_USERNAME = creds_dict["USERNAME"]
MQTT_PASSWORD = creds_dict["PASSWORD"]

# Configuration for Kafka (AWS MSK)
KAFKA_BROKER_URL = "b-1.democluster1.iv249r.c3.kafka.us-west-1.amazonaws.com:9098"  # Replace with your MSK cluster's broker URL
KAFKA_TOPIC = "DustSensorTopic"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER_URL],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    security_protocol="SSL"
)

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc, props=None):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2, MQTT_CLIENT, protocol=4)
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.connect(MQTT_BROKER, MQTT_PORT)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    client.subscribe(MQTT_TOPIC)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()


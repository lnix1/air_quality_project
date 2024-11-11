# Basic template taken from: https://github.com/maxcotec/aws-IAM-auth-msk-python/blob/main/main.py

import argparse
import threading
from creds import creds_dict
from paho.mqtt import client as mqtt_client

import subprocess as subp

# Configuration for ThingSpeak's MQTT
MQTT_BROKER = creds_dict["MQTT_BROKER"]
MQTT_PORT = creds_dict["MQTT_PORT"] 
MQTT_TOPIC = creds_dict["MQTT_TOPIC"] 
MQTT_CLIENT = creds_dict["MQTT_CLIENT"]
MQTT_USERNAME = creds_dict["MQTT_USERNAME"]
MQTT_PASSWORD = creds_dict["MQTT_PASSWORD"]

def parse_arguments():
    """ parse the arguments of the script """
    parser = argparse.ArgumentParser(description='Kafka Sub Pub')

    # Required Arguments
    required_parser = parser.add_argument_group(title='required arguments')

    required_parser.add_argument('--kafka-servers', help="kafka servers (comma seperated)")
    required_parser.add_argument('--pub-topic', required=True, help="topic where to publish data to")
    required_parser.add_argument('--configs', required=False,
                                 help="custom client properties to enable IAM auth enabled kafka cluster.")

    # optional
    parser.add_argument('--kafka-path', default="/home/ec2-user/kafka_2.13-3.6.0",
                        help="location where kafka is installed")
    parser.add_argument('--aws-region', default="us-west-1", help="aws region")
    return parser.parse_args()


def create_cli_consumer():
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

def create_cli_producer(arguments):
    print(f"Initializing kafka producer for servers: {arguments.kafka_servers}")
    print(f"topic: {arguments.pub_topic}")

    kafka_producer_init_cmd = [
        f"{arguments.kafka_path}/bin/kafka-console-producer.sh",
        "--topic", arguments.pub_topic,
        "--bootstrap-server", arguments.kafka_servers
    ]

    if arguments.configs:
        kafka_producer_init_cmd = kafka_producer_init_cmd + ["--producer.config", arguments.configs]

    try:
        proc = subp.Popen(kafka_producer_init_cmd, stdin=subp.PIPE)
        print("kafka producer init done.")
        return proc
    except Exception as e:
        print(f"Error creating producer: {e}")
        return None


# Define a function to consume messages
def consume_messages(consumer, producer):
    def on_message(client, userdata, msg):
        message = msg.payload.decode()
        send_msg_thread = threading.Thread(target=send_message, args=(producer, message))
        send_msg_thread.start()

    consumer.subscribe(MQTT_TOPIC)
    consumer.on_message = on_message
    consumer.loop_start()

def send_message(producer, msg):
    # Publish the received message to the producer
    try:
        print(f"Publishing message: {msg}")
        producer.stdin.write(msg.encode() + b"\n")
        producer.stdin.flush()
    except Exception as e:
        print(f"Error sending message: {e}")


def main():
    args = parse_arguments()

    # Create the producer process in a separate thread
    kafka_producer = create_cli_producer(args)

    # Create the consumer process
    kafka_consumer = create_cli_consumer()

    # Start the Kafka consumer thread
    consumer_thread = threading.Thread(target=consume_messages, args=(kafka_consumer, kafka_producer))
    consumer_thread.daemon = True
    consumer_thread.start()

    # Your main program logic can continue here while the consumer and producer threads are running

    # For example, you can add a loop to keep the main thread alive or perform other operations.
    while True:
        pass


if __name__ == "__main__":
    main()

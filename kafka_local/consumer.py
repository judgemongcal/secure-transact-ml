from confluent_kafka import Consumer
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_CONFIG ={
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'transaction-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'session.timeout.ms': 120000,
    'heartbeat.interval.ms': 5000
}

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}


consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe(['transactions'])

def consume_messages():
    '''
    Consumes messages from Kafka topic.
    '''

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue;
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue;

        print(f"Received Transaction Batch: {msg.value().decode('utf-8')}")
        consumer.commit()
consume_messages()
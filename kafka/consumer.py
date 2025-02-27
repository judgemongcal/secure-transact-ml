from confluent_kafka import Consumer

KAFKA_CONFIG ={
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'transaction-consumer-group',
    'auto.offset.reset': 'earliest',
    'session.timeout.ms': 120000
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

consume_messages()
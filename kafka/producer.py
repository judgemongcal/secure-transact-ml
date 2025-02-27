from confluent_kafka import Producer


KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092'
}



producer = Producer(KAFKA_CONFIG)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def send_to_kafka(topic, message):
    '''
    Sends a message to a Kafka topic.
    '''
    producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
    producer.flush()



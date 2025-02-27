from confluent_kafka import Producer


KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(KAFKA_CONFIG)

def send_to_kafka(topic, message):
    '''
    Sends a message to a Kafka topic.
    '''
    producer.produce(topic, message.encode('utf-8'))
    producer.flush()



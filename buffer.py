from kafka import KafkaProducer

from kafka_wrapper import Buffer
from constants import BOOTSTRAP_SERVER, TOPIC

def add(a, b):
    return a + b


# Set up a Kafka producer.
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)

# Set up a queue.
buffer = Buffer(topic=TOPIC, producer=producer)

# Enqueue a function call.
job = buffer.enqueue(add, 1, 2)

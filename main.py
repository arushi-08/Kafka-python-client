import requests

from kafka import KafkaProducer
from kafka_wrapper import Queue

# Set up a Kafka producer.
producer = KafkaProducer(bootstrap_servers="127.0.0.1:9092")

# Set up a queue.
queue = Queue(topic="topic", producer=producer)
# Enqueue a function call.
def multiply(a, b):
    return a * b

# job = queue.enqueue(requests.get, "https://google.com")
job = queue.enqueue(multiply, 3, 5)
# You can also specify the job timeout, Kafka message key and partition.
# job = queue.using(timeout=5, key=b"foo", partition=0).enqueue(requests.get, "https://google.com")
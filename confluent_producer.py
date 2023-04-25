import time
from confluent_kafka import Producer
from kafka_wrapper import Buffer
from constants import BOOTSTRAP_SERVER, TOPIC
from kafka_wrapper import Job

conf = {'bootstrap.servers': BOOTSTRAP_SERVER,
        'queue.buffering.max.messages': 10000000}  # set the max number of messages to buffer}

producer = Producer(**conf)

buffer = Buffer(topic=TOPIC, producer=producer, config=conf)

def sum(a, b):
    return a + b

for i in range(30):
    n_partition = i % 3 # distribute messages across 3 partitions
    job = Job(
        func=sum,
        args=(i, 10),
        partition=n_partition,
    )
    job = buffer.push(job, sum, i, 10)
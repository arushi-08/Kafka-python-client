import time
import pandas as pd
from confluent_kafka import Producer
from kafka_wrapper import Buffer
from constants import BOOTSTRAP_SERVER, TOPIC
from kafka_wrapper import Job

conf = {'bootstrap.servers': BOOTSTRAP_SERVER,
        'queue.buffering.max.messages': 10000000}  # set the max number of messages to buffer}

producer = Producer(**conf)

buffer = Buffer(topic=TOPIC, producer=producer, config=conf)

def job(msg):
    print(msg)


def python_kafka_producer_performance():
    conf = {'bootstrap.servers': BOOTSTRAP_SERVER,
        'queue.buffering.max.messages': 10000000}  # set the max number of messages to buffer}
    producer = Producer(**conf)
    buffer = Buffer(topic=TOPIC, producer=producer, config=conf)
    msg_count = 1000000
    msg_size = 100
    msg_payload = ('kafkatest' * 20).encode()[:msg_size]
    
    producer_start = time.time()
    total_job_creation_time = []
    total_producer_send_time = []
    for i in range(msg_count):
        # msg_payload, total_producer_send_time, total_job_creation_time, obj, *args, **kwargs
        buffer.test_push(
            msg_payload, 
            total_producer_send_time, 
            total_job_creation_time, 
            job
            )
    producer.flush()
    print(f"Check total_producer_send_time {sum(total_producer_send_time)}")
    return time.time() - producer_start

def producer_benchmark():
    producer_timings = {}
    producer_timings['confluent_python_kafka_producer'] = python_kafka_producer_performance()
    calculate_thoughput(producer_timings['confluent_python_kafka_producer'])
    return producer_timings

def calculate_thoughput(timing, n_messages=1000000, msg_size=100):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024*1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))


if __name__ == '__main__':
    producer_timings = producer_benchmark()
    producer_df = (pd.DataFrame
                   .from_dict(producer_timings, orient='index')
                   .rename(columns={0: 'time_in_seconds'}))
    print("producer_performance")
    print(producer_df)
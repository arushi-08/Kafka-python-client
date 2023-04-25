import dill
import uuid
import time
import pandas as pd
from confluent_kafka import Producer, Consumer
from kafka_wrapper import Buffer
from kafka_wrapper import Worker
from constants import BOOTSTRAP_SERVER, TOPIC, GROUP_ID

def calculate_thoughput(timing, n_messages=1000000, msg_size=100):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024*1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))

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
        buffer.test_push(msg_payload, total_producer_send_time, total_job_creation_time, True, job, msg_payload)  ## difference
    producer.flush()
    print(f"Check total_producer_send_time {sum(total_producer_send_time)}")
    return time.time() - producer_start

def deserializer(serialized):
    """Example deserializer function with extra sanity checking."""
    assert isinstance(serialized, bytes), "Expecting a bytes"
    return dill.loads(serialized)

def python_kafka_consumer_performance():
    print("inside python_kafka_consumer_performance")
    msg_count = 1000000
    conf = {'bootstrap.servers': BOOTSTRAP_SERVER,
            'group.id': uuid.uuid1(),
            'session.timeout.ms': 6000,
            'default.topic.config': {
                'auto.offset.reset': 'earliest'
            }
    }
    # TODO: need to add worker
    consumer = Consumer(**conf)
    worker = Worker(
        topic=TOPIC, consumer=consumer, config=conf, deserializer=deserializer
    )
    consumer_timing = worker.start_process(max_messages=msg_count, test=True)
    return consumer_timing

def producer_benchmark():
    producer_timings = {}
    producer_timings['confluent_python_kafka_producer'] = python_kafka_producer_performance()
    calculate_thoughput(producer_timings['confluent_python_kafka_producer'])
    return producer_timings

def consumer_benchmark():
    print("inside consumer_benchmark")
    consumer_timings = {}
    consumer_timings['confluent_python_kafka_consumer'] = python_kafka_consumer_performance()
    calculate_thoughput(consumer_timings['confluent_python_kafka_consumer'])
    return consumer_timings

if __name__ == '__main__':
    # producer_timings = producer_benchmark()
    # producer_df = (pd.DataFrame
    #                .from_dict(producer_timings, orient='index')
    #                .rename(columns={0: 'time_in_seconds'}))
    # producer_df.to_csv('confluent_producer_performance_run_3.csv', index=False)

    consumer_timings = consumer_benchmark()
    consumer_df = (pd.DataFrame
                   .from_dict(consumer_timings, orient='index')
                   .rename(columns={0: 'time_in_seconds'}))
    consumer_df.to_csv('confluent_consumer_performance.csv', index=False)

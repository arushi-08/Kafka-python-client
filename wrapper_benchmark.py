import time
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from kafka_wrapper import Buffer

def calculate_thoughput(timing, n_messages=1000000, msg_size=100):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024*1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))

def job(msg):
    print(msg)

def python_kafka_producer_performance():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    topic = 'python-kafka-topic'
    buffer = Buffer(topic=topic, producer=producer)
    msg_count = 1000000
    msg_size = 100
    msg_payload = ('kafkatest' * 20).encode()[:msg_size]
    
    producer_start = time.time()
    for i in range(msg_count):
        buffer.enqueue(job, msg_payload)
        
    return time.time() - producer_start


def python_kafka_consumer_performance():
    topic = 'python-kafka-topic'
    
    msg_count = 1000000
    consumer = KafkaConsumer(
        bootstrap_servers='localhost:9092',
        auto_offset_reset = 'earliest',
        group_id = None
    )
    msg_consumed_count = 0
    consumer_start = time.time()
    consumer.subscribe([topic])
    for msg in consumer:
        msg_consumed_count += 1
        if msg_consumed_count >= msg_count:
            break
                    
    consumer_timing = time.time() - consumer_start
    consumer.close()    
    return consumer_timing

def producer_benchmark():
    producer_timings = {}
    producer_timings['python_kafka_producer'] = python_kafka_producer_performance()
    calculate_thoughput(producer_timings['python_kafka_producer'])
    return producer_timings

def consumer_benchmark():
    consumer_timings = {}
    _ = python_kafka_consumer_performance()
    consumer_timings['python_kafka_consumer'] = python_kafka_consumer_performance()
    calculate_thoughput(consumer_timings['python_kafka_consumer'])
    return consumer_timings

if __name__ == '__main__':
    producer_timings = producer_benchmark()
    producer_df = (pd.DataFrame
                   .from_dict(producer_timings, orient='index')
                   .rename(columns={0: 'time_in_seconds'}))
    producer_df.to_csv('producer_performance.csv', index=False)

    # consumer_timings = consumer_benchmark()
    # consumer_df = (pd.DataFrame
    #                .from_dict(consumer_timings, orient='index')
    #                .rename(columns={0: 'time_in_seconds'}))
    # consumer_df.to_csv('consumer_performance.csv', index=False)

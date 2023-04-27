import logging
import uuid
import dill
import pandas as pd
from confluent_kafka import Consumer
from kafka_wrapper import Worker
from constants import BOOTSTRAP_SERVER, TOPIC

# Set up logging
formatter = logging.Formatter(
    fmt="[%(asctime)s][%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger = logging.getLogger("kafka_wrapper.worker")
logger.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

def deserializer(serialized):
    """Example deserializer function with extra sanity checking."""
    assert isinstance(serialized, bytes), "Expecting a bytes"
    return dill.loads(serialized)


def consumer_benchmark():
    consumer_timings = {}
    consumer_timings['confluent_python_kafka_consumer'] = python_kafka_consumer_performance()
    calculate_thoughput(consumer_timings['confluent_python_kafka_consumer'])
    return consumer_timings

def calculate_thoughput(timing, n_messages=1000000, msg_size=100):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024*1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))


def python_kafka_consumer_performance():
    msg_count = 1000000
    conf = {'bootstrap.servers': BOOTSTRAP_SERVER,
            'group.id': uuid.uuid1(),
            'session.timeout.ms': 6000,
            'default.topic.config': {
                'auto.offset.reset': 'earliest'
            }
    }
    consumer = Consumer(**conf)
    worker = Worker(
        topic=TOPIC, consumer=consumer, config=conf, deserializer=deserializer
    )
    consumer_timing = worker.start_process(max_messages=msg_count, test=True)
    return consumer_timing


if __name__ == "__main__":
    consumer_timings = consumer_benchmark()
    consumer_df = (pd.DataFrame
                   .from_dict(consumer_timings, orient='index')
                   .rename(columns={0: 'time_in_seconds'}))
    print("consumer_performance")
    print(consumer_df)

import logging
import dill
from confluent_kafka import Consumer
from kafka_wrapper import Worker
from constants import BOOTSTRAP_SERVER, GROUP_ID, TOPIC

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


if __name__ == "__main__":
    conf = {'bootstrap.servers': BOOTSTRAP_SERVER,
            'group.id': GROUP_ID,
            'session.timeout.ms': 6000,
            'default.topic.config': {
                'auto.offset.reset': 'earliest'
            }
    }
    consumer = Consumer(**conf)
    worker = Worker(
        topic=TOPIC, consumer=consumer, config=conf, deserializer=deserializer
    )
    page_views = {}
    worker.start_process(page_views)
    print("Continue with main thread")

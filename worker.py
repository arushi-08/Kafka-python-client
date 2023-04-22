import logging

import dill
from kafka import KafkaConsumer

from kafka_wrapper import Worker
from constants import BOOTSTRAP_SERVER, GROUP_ID, TOPIC

# Set up logging
formatter = logging.Formatter(
    fmt="[%(asctime)s][%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger = logging.getLogger("kq.worker")
logger.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

def deserializer(serialized):
    """Example deserializer function with extra sanity checking."""
    assert isinstance(serialized, bytes), "Expecting a bytes"
    return dill.loads(serialized)


if __name__ == "__main__":
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    worker = Worker(
        topic=TOPIC, consumer=consumer, deserializer=deserializer
    )
    worker.start()

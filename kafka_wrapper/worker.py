import _thread
import logging
import threading
import traceback

import dill
from kafka import KafkaConsumer

from kafka_wrapper.job import Job
from kafka_wrapper.message import Message
from kafka_wrapper.utils import get_call_repr


class Worker:
    """Fetches :doc:`jobs <job>` from Kafka topics and processes them."""
    def __init__(
        self,
        topic: str,
        consumer: KafkaConsumer,
        deserializer = None,
        logger = None,
    ):
        self._topic = topic
        self._hosts = consumer.config["bootstrap_servers"]
        self._group = consumer.config["group_id"]
        self._consumer = consumer
        if deserializer:
            self._deserializer = deserializer
        else:
            self._deserializer = dill.loads
        if logger:
            self._logger = logger
        else:
            self._logger = logging.getLogger("kq.worker")

    def __repr__(self) -> str:
        """Return the string representation of the worker."""
        return f"Worker(hosts={self._hosts}, topic={self._topic}, group={self._group})"

    def __del__(self) -> None:
        try:
            self._consumer.close()
        except Exception:
            pass

    def _process_message(self, msg: Message) -> None:
        """De-serialize the message and execute the job."""
        self._logger.info(
            "Processing Message(topic={}, partition={}, offset={}) ...".format(
                msg.topic, msg.partition, msg.offset
            )
        )
        try:
            job = self._deserializer(msg.value)
            job_repr = get_call_repr(job.func, *job.args, **job.kwargs)

        except Exception as err:
            self._logger.exception(f"Job was invalid: {err}")
            job = None
        else:
            self._logger.info(f"Executing job {job.id}: {job_repr}")

            if job.timeout:
                timer = threading.Timer(job.timeout, _thread.interrupt_main)
                timer.start()
            else:
                timer = None
            try:
                res = job.func(*job.args, **job.kwargs)
            except KeyboardInterrupt:
                self._logger.error(f"Job {job.id} timed out or was interrupted")
                assert isinstance(job, Job)

            except Exception as err:
                self._logger.exception(f"Job {job.id} raised an exception:")
                tb = traceback.format_exc()
                assert isinstance(job, Job)
            else:
                self._logger.info(f"Job {job.id} returned: {res}")
                assert isinstance(job, Job)
            finally:
                if timer is not None:
                    timer.cancel()

    @property
    def topic(self):
        """Return the name of the Kafka topic."""
        return self._topic

    @property
    def consumer(self):
        """Return the Kafka consumer instance."""
        return self._consumer

    @property
    def deserializer(self):
        """Return the deserializer function."""
        return self._deserializer

    def start(
        self, max_messages = None, commit_offsets = True
    ):
        """Start processing Kafka messages and executing jobs."""
        self._logger.info(f"Started {self}")

        self._consumer.unsubscribe()
        self._consumer.subscribe([self.topic])

        messages_processed = 0
        while max_messages is None or messages_processed < max_messages:
            record = next(self._consumer)
            message = Message(
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                key=record.key,
                value=record.value,
            )
            self._process_message(message)

            if commit_offsets:
                self._consumer.commit()

            messages_processed += 1

        return messages_processed

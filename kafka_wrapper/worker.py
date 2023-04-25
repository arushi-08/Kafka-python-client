import time
import _thread
import logging
import threading
import traceback

import dill
from confluent_kafka import Consumer, KafkaError

from kafka_wrapper.job import Job
from kafka_wrapper.message import Message
from kafka_wrapper.utils import get_call_repr


class Worker:
    """Fetches :doc:`jobs <job>` from Kafka topics and processes them."""
    def __init__(
        self,
        topic: str,
        consumer: Consumer,
        config = {},
        deserializer = None,
        logger = None,
    ):
        self._topic = topic
        self._hosts = config["bootstrap.servers"]
        self._group = config["group.id"]
        self._consumer = consumer
        if deserializer:
            self._deserializer = deserializer
        else:
            self._deserializer = dill.loads
        if logger:
            self._logger = logger
        else:
            self._logger = logging.getLogger("kafka_wrapper.worker")

    def __repr__(self) -> str:
        """Return the string representation of the worker."""
        return f"Worker(hosts={self._hosts}, topic={self._topic}, group={self._group})"

    def __del__(self) -> None:
        try:
            self._consumer.close()
        except Exception:
            pass

    def _process_message(self, msg: Message, *args) -> None:
        """De-serialize the message and execute the job."""
        self._logger.info(
            "Processing Message(topic={}, partition={}, offset={}) ...".format(
                msg.topic(), msg.partition(), msg.offset()
            )
        )
        try:
            job = self._deserializer(msg.value())
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
                if args:
                    res = job.func(args[0], *job.args, **job.kwargs)
                else:
                    res = job.func(args[0], *job.args, **job.kwargs)
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

    def start_process(
        self, *args, max_messages = None, commit_offsets = True, test=False,
    ):
        """Start processing Kafka messages and executing jobs."""
        
        self._logger.info(f"Started {self}")

        self._consumer.unsubscribe()
        if test:
            msg_consumed_count = 0
            consumer_start = time.time()
            self._consumer.subscribe([self.topic])
            while True:
                msg = self._consumer.poll(1)
                if msg:
                    msg_consumed_count += 1
                
                if msg_consumed_count >= max_messages:
                    break
            consumer_timing = time.time() - consumer_start
            self._consumer.close() 
            return consumer_timing

        self._consumer.subscribe([self.topic])
        messages_processed = 0

        while max_messages is None or messages_processed < max_messages:
            # record = next(self._consumer)
            record = self._consumer.poll(1.0)
            if record is None:
                continue
            if record.error():
                if record.error().code() == KafkaError._PARTITION_EOF:
                    self._logger.error('End of partition reached')
                else:
                    self._logger.error('Error: %s' % record.error())
                    
            message = Message(
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                key=record.key,
                value=record.value,
            )
            if args:
                self._process_message(message, args[0])
            else:
                self._process_message(message)

            if commit_offsets:
                self._consumer.commit()

            messages_processed += 1

        self._logger.info(f"Processed {messages_processed} Messages")
        
        return messages_processed

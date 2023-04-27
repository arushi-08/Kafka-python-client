import logging
import time
import uuid

import dill
from kafka import KafkaProducer

from kafka_wrapper import Job

class Buffer:
    """Pushes function calls in Kafka topics as :doc:`jobs <job>`."""

    def __init__(
        self,
        topic,
        producer,
        config = {},
        serializer = None,
        timeout = 0,
        logger = None,
        key = None,
        partition = None
    ) -> None:

        self._topic = topic
        self._hosts: str = config["bootstrap.servers"]
        self._producer = producer
        self._serializer = serializer or dill.dumps
        self._timeout = timeout
        self._logger = logger or logging.getLogger("kafka_wrapper.queue")
        self._key = key
        self._partition = partition

    def __repr__(self) -> str:
        """Return the string representation of the queue."""
        return f"Queue(hosts={self._hosts}, topic={self._topic})"

    def __del__(self) -> None:
        try:
            self._producer.close()
        except Exception:
            pass

    @property
    def hosts(self) -> str:
        """Return comma-separated Kafka hosts and ports string."""
        return self._hosts

    @property
    def topic(self) -> str:
        """Return the name of the Kafka topic."""
        return self._topic

    @property
    def producer(self) -> KafkaProducer:
        """Return the Kafka producer instance."""
        return self._producer

    @property
    def serializer(self):
        """Return the serializer function."""
        return self._serializer

    @property
    def timeout(self):
        """Return the default job timeout threshold in seconds."""
        return self._timeout

    def push(self, obj, *args, **kwargs):
        """Push a function call or a job"""
        timestamp = int(time.time() * 1000)

        if isinstance(obj, Job):
            if obj.id is None:
                job_id = uuid.uuid4().hex
            else:
                job_id = obj.id

            if obj.args is None:
                args = tuple()
            else:
                args = tuple(obj.args)

            if not callable(obj.func):
                raise Exception("obj.func argument must be a callable")
            
            func = obj.func
            kwargs = {} if obj.kwargs is None else obj.kwargs
            timeout = self._timeout if obj.timeout is None else obj.timeout
            key = self._key if obj.key is None else obj.key
            part = self._partition if obj.partition is None else obj.partition

        else:
            if not callable(obj):
                raise Exception("first argument must be a callable")
            
            job_id = uuid.uuid4().hex
            func = obj
            args = args
            kwargs = kwargs
            timeout = self._timeout
            key = self._key
            part = self._partition

        job = Job(
            id=job_id,
            timestamp=timestamp,
            topic=self._topic,
            func=func,
            args=args,
            kwargs=kwargs,
            timeout=timeout,
            key=key,
            partition=part,
        )
        self._logger.info(f"Pushing {job} ...")
        self._producer.produce(
            self._topic,
            value=self._serializer(job),
            # key=self._serializer(key) if key else None,
            partition=part,
            # timestamp_ms=timestamp,
        )
        self._producer.flush()
        return job


    def test_push(self, msg_payload, total_producer_send_time, total_job_creation_time, obj, *args, **kwargs):
        """Push a function call or a job"""
        timestamp = int(time.time() * 1000)

        if isinstance(obj, Job):
            if obj.id is None:
                job_id = uuid.uuid4().hex
            else:
                job_id = obj.id

            if obj.args is None:
                args = tuple()
            else:
                args = tuple(obj.args)

            if not callable(obj.func):
                raise Exception("obj.func argument must be a callable")
            
            func = obj.func
            kwargs = {} if obj.kwargs is None else obj.kwargs
            timeout = self._timeout if obj.timeout is None else obj.timeout
            key = self._key if obj.key is None else obj.key
            part = self._partition if obj.partition is None else obj.partition

        else:
            if not callable(obj):
                raise Exception("first argument must be a callable")
            
            job_id = uuid.uuid4().hex
            func = obj
            args = args
            kwargs = kwargs
            timeout = self._timeout
            key = self._key
            part = self._partition
        job_start_time  = time.time()
        job = Job(
            id=job_id,
            timestamp=timestamp,
            topic=self._topic,
            func=func,
            args=args,
            kwargs=kwargs,
            timeout=timeout,
            key=key,
            partition=part,
        )
        total_job_creation_time.append(time.time() - job_start_time)
        # self._logger.info(f"Time taken to create job {time.time() - job_start_time}")
        self._logger.info(f"Pushing {job} ...")
        
        produce_start_time  = time.time()
        self._producer.produce(
            self._topic,
            value=msg_payload,
            # key=self._serializer(key) if key else None,
            # partition=part,
            # timestamp_ms=timestamp,
        )
        self._producer.poll(0)
        total_producer_send_time.append(time.time() - produce_start_time)
        
        return job

# Kafka Client for Python

This is a lightweight Python library which lets you enqueue and
execute jobs asynchronously using [Apache Kafka](https://kafka.apache.org/). It uses
[kafka-python](https://github.com/dpkp/kafka-python) under the hood.

## Requirements

* [Apache Kafka](https://kafka.apache.org) 0.9+
* Python 3.6+

## Getting Started

Start your Kafka instance. 

```shell
docker run -p 9092:9092 <fix this>
```


Start your worker:

```shell
python my_worker.py
[INFO] Starting Worker(hosts=127.0.0.1:9092 topic=topic, group=group) ...
```

Enqueue a function call:

```python
import requests

from kafka import KafkaProducer
from kafka_wrapper import Queue

# Set up a Kafka producer.
producer = KafkaProducer(bootstrap_servers="127.0.0.1:9092")

# Set up a queue.
queue = Queue(topic="topic", producer=producer)
# Enqueue a function call.
def multiply(a, b):
    return a * b

job = queue.enqueue(multiply, 3, 5)
```

The worker executes the job in the background:

```shell
python my_worker.py
[INFO] Starting Worker(hosts=127.0.0.1:9092, topic=topic, group=group) ...
[INFO] Processing Message(topic=topic, partition=0, offset=0) ...
[INFO] Executing job e68e7a40b4944ac18f4c3926f31acf07: __main__.multiply(3, 5)
[2023-04-21 00:31:22][INFO] Job e68e7a40b4944ac18f4c3926f31acf07 returned: 15
```

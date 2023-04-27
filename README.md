# Kafka Client for Python

This is a lightweight Python library which lets you enqueue and
execute jobs asynchronously using [Apache Kafka](https://kafka.apache.org/). It uses
[kafka-python](https://github.com/dpkp/kafka-python) under the hood.

## Requirements

* [Apache Kafka](https://kafka.apache.org) 3.4.0
* Python 3.10+

## Getting Started

Start your Kafka instance. 

```shell
docker run -p 9092:9092 <fix this>
```


Start the buffer. This will connect to the KafkaProducer client:

```shell
python confluent_kafka_producer_flask.py
```

Start the workers in separate terminal. This will connect to the KafkaProducer client. The worker executes the job in the background:

```shell
python confluent_consumer.py
[INFO] Starting Worker(hosts=127.0.0.1:9092 topic=page-views, group=group) ...
```

To create a consumer group, running multiple consumer instances that will execute subset of jobs, we need to increase the number of partitions. Following is example command that increases number of partitions to 3 of the page-views Kafka topic.

```shell
 bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic page-views --partitions 3
 ```

[INFO] Executing job e68e7a40b4944ac18f4c3926f31acf07: __main__.multiply(3, 5)
[2023-04-21 00:31:22][INFO] Job e68e7a40b4944ac18f4c3926f31acf07 returned: 15
```

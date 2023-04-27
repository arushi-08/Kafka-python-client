# Confluent Kafka Python Client Wrapper for Python

This is a lightweight Python library which lets you push and
execute jobs asynchronously using [Apache Kafka](https://kafka.apache.org/). \
It uses
[confluent-kafka-python](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html) under the hood.

## Requirements

* [Apache Kafka](https://kafka.apache.org) 3.4.0
* Python 3.10+

## Getting Started
Problem statement: A real-time dashboard that displays the number of page views per second for a website. 
The website generates a high volume of page views, and you want to process them in real-time.


Setup: \
Trigger_Requests.py : simulates 1000 users requesting our website. \
confluent_kafka_producer_flask.py: sends the “page view” events in real-time to Kafka Server.  \
confluent_consumer.py: will call the Worker class and consume page view events. \
Each consumer instance processes a subset of the messages.


Start your Kafka instance. 

```shell
docker run -p 9092:9092 <fix this>
```
 
 ## Performance Evaluation Scripts
 
 ```shell
python producer_benchmark.py
Check total_producer_send_time 0.6862080097198486
Processed 1000000 messsages in 4.16 seconds
22.92 MB/s
240303.52 Msgs/s
producer_performance
                                 time_in_seconds
confluent_python_kafka_producer         4.161404
```

 
 ```shell
python consumer_benchmark.py 
[2023-04-27 18:02:40][INFO] Started Worker(hosts=127.0.0.1:9092, topic=page-views, group=3a66ae62-e547-11ed-808d-264d3686d37d)
Processed 1000000 messsages in 4.19 seconds
22.75 MB/s
238511.94 Msgs/s
consumer_performance
                                 time_in_seconds
confluent_python_kafka_consumer         4.192662
```
 
 
 ## Demo: Real-time Calculation of Page Views of our website
 
 
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
 

[![IMAGE ALT TEXT HERE](https://img.youtube.com/vi/gzsFujHQkUU/0.jpg)](https://youtu.be/gzsFujHQkUU)


```

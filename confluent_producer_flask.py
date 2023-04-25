import time
from flask import Flask, request
from confluent_kafka import Producer
from kafka_wrapper import Buffer
from constants import BOOTSTRAP_SERVER, TOPIC
from kafka_wrapper import Job

app = Flask(__name__)

# create a Kafka producer
# producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


conf = {'bootstrap.servers': BOOTSTRAP_SERVER,
        'queue.buffering.max.messages': 10000000}  # set the max number of messages to buffer}

producer = Producer(**conf)

buffer = Buffer(topic=TOPIC, producer=producer, config=conf)

# def sum(a, b):
#     return a + b

# for i in range(30):
#     n_partition = i % 3 # distribute messages across 3 partitions
#     job = Job(
#         func=sum,
#         args=(i, 10),
#         partition=n_partition,
#     )
#     job = buffer.push(job, sum, i, 10)


n_partition = 3
n_jobs = 0

def count_page_views(page_views, page):
    page_views[page] = page_views.get(page, 0) + 1
    return page_views

@app.route('/')
def index():
    global n_partition
    global n_jobs   
    job = Job(
        func=count_page_views,
        args=request.path,
        partition=n_jobs % n_partition,
    )
    job = buffer.push(job)
    n_jobs += 1
    return 'Hello World!'

if __name__ == '__main__':
    app.run(debug=True)
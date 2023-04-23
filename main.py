# import requests

from kafka import KafkaProducer
from kafka_wrapper import Buffer
from constants import BOOTSTRAP_SERVER, TOPIC

# Set up a Kafka producer.
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)

# Set up a queue.
buffer = Buffer(topic=TOPIC, producer=producer)
# Enqueue a function call.
def multiply(a, b):
    return a * b
def sum(a, b):
    return a + b

# import time
# start = time.time()
# async def sleep():
#     print(f'Time: {time.time() - start:.2f}')
#     await time.sleep(1)

# async def sum(name, numbers):
#     total = 0
#     for number in numbers:
#         print(f'Task {name}: Computing {total}+{number}')
#         await sleep()
#         total += number
#     print(f'Task {name}: Sum = {total}\n')


import asyncio
from time import sleep

def job1():
    print("Starting job 1")
    sleep(2)
    print("Job 1 completed")

def job2():
    print("Starting job 2")
    sleep(1)
    print("Job 2 completed")

def dummy_main():
    print("Starting main")
    [job1(), job2()]
    print("Main completed")

# job = buffer.enqueue(requests.get, "https://google.com")
job = buffer.enqueue(sum, 2, 5) # 10 # 7
# job = buffer.using(partition=1).enqueue(sum, 3, 5) # 15 # 8
# job = buffer.using(partition=2).enqueue(sum, 4, 5) # 20 # 9
# job = buffer.using(partition=3).enqueue(sum, 5, 5) # 25 # 10
# job = buffer.enqueue(asyncio.run, dummy_main())
# job = buffer.enqueue(dummy_main)
# You can also specify the job timeout, Kafka message key and partition.
# job = buffer.using(timeout=5, key=b"foo", partition=0).enqueue(requests.get, "https://google.com")
from kafka import KafkaConsumer
from constants import TOPIC

consumer = KafkaConsumer(TOPIC)

print("Starting consumer...")
for msg in consumer:
    print(msg)

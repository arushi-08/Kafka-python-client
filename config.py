import logging

from confluent_kafka.admin import AdminClient, NewPartitions
from constants import BOOTSTRAP_SERVER, TOPIC, NUM_PARTITION

def config_kafka_admin_client():
    # Configure Kafka admin client
    conf = {'bootstrap.servers': BOOTSTRAP_SERVER}
    admin_client = AdminClient(conf)

    # Get current metadata for Kafka topic
    topic_metadata = admin_client.list_topics(topic=TOPIC)

    # Get current partition count for Kafka topic
    partition_count = len(topic_metadata.topics[TOPIC].partitions)

    # Create new partitions for Kafka topic
    if NUM_PARTITION > partition_count:
        new_partitions = NewPartitions(TOPIC, new_total_count=NUM_PARTITION)
        new_partitions.replica_assignment = [[p] for p in range(partition_count, NUM_PARTITION)]
        admin_client.create_partitions(new_partitions=[new_partitions])

    print(f'Kafka new config is set: {len(topic_metadata.topics[TOPIC].partitions)} partitions')


def get_current_partition_count():
    # Configure Kafka admin client
    conf = {'bootstrap.servers': BOOTSTRAP_SERVER}
    admin_client = AdminClient(conf)

    # Get current metadata for Kafka topic
    topic_metadata = admin_client.list_topics(topic=TOPIC)

    # Get current partition count for Kafka topic
    partition_count = len(topic_metadata.topics[TOPIC].partitions)

    print(f'Current topic: {TOPIC} partition count: {partition_count}')

if __name__ == '__main__':
    config_kafka_admin_client()
    
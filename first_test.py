"""
1) in cmd you have to run:
kafka-topics.sh --create --zookeeper zookeeper:2181 --partitions 1 --replication-factor 1 --topic egor
2) next step is checking how many we have topics
kafka-topics.sh --list --zookeeper zookeeper:2181
3) read the latest messages from Kafka topic
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic egor :TODO do not need anymore
"""

from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random

kafka_topic_name = "egor"
kafka_bootstrap_server = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=kafka_topic_name,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

    transaction_card_type_list = ["Visa", "MasterCard", "Maestro"]

    for i in range(10):
        i = i + 1
        message = {}
        print("Sending message to Kafka topic: " + str(i))
        event_datetime = datetime.now()

        message["transaction_id"] = str(i)
        message["transaction_card_type"] = random.choice(transaction_card_type_list)
        message["transaction_amount"] = round(random.uniform(5.5, 555.5), 2)
        message["transaction_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")

        print("Message to be sent: ", message)
        kafka_producer_obj.send(kafka_topic_name, message)
        time.sleep(1)

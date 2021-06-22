from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'weather_file',  # TODO
    bootstrap_servers=['localhost:9092'],
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    value = message.value
    print(f'{value}')

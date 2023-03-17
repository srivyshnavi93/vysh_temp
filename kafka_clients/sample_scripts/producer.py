from time import sleep
from json import dumps
from kafka import KafkaProducer

import random

my_producer = KafkaProducer(bootstrap_servers = ['kafka-broker:9092'], value_serializer = lambda x:dumps(x).encode('utf-8'))

# for n in range(200, 250):
while True:
    n = random.choice([1, 2, 3, 4, 5, 6, 7, 8, 9, 0])
    my_data = {'data' : str(n)}
    print(my_data)
    my_producer.send(topic = 'test-topic-1', value = my_data)
    sleep(10)
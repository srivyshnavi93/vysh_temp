from time import sleep
from json import dumps
from kafka import KafkaProducer

my_producer = KafkaProducer(bootstrap_servers = ['kafka-broker:9092'], value_serializer = lambda x:dumps(x).encode('utf-8'))

for n in range(200, 250):
    my_data = {'num' : n}
    print(my_data)
    my_producer.send(topic = 'test-topic-1', value = my_data)
    sleep(1)
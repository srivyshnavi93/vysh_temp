from time import sleep
from json import dumps, loads
from kafka import KafkaConsumer

my_consumer = KafkaConsumer('test-topic-1', bootstrap_servers = ['kafka-broker:9092'], auto_offset_reset = 'earliest', enable_auto_commit = True, group_id = 'test-consumer-group-1', value_deserializer = lambda x : loads(x.decode('utf-8')))

for m in my_consumer:
        print(m.value)
        sleep(1)
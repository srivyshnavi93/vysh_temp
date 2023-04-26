from time import sleep
from json import dumps, loads
from kafka import KafkaConsumer

topic_name = 'test-topic-1'
#bootstrap_servers=['kafka-broker-1:9092']
bootstrap_servers=['kafka-broker-1-service:9092']
group_id='test-consumer-group-1'

my_consumer = KafkaConsumer(topic_name,
                            bootstrap_servers=bootstrap_servers,
                            group_id=group_id,
                            auto_offset_reset='earliest',
                            enable_auto_commit=True,
                            value_deserializer=lambda x: loads(x.decode('utf-8'))
                            )

for m in my_consumer:
    print(m.value)
    sleep(1)

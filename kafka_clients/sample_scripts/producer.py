from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

#admin_client = KafkaAdminClient(bootstrap_servers="kafka-broker-2:9092")
#topic_list = []
#topic_list.append(NewTopic(name="test-topic-2", num_partitions=3, replication_factor=2))
#admin_client.create_topics(new_topics=topic_list, validate_only=False)

import random

my_producer = KafkaProducer(bootstrap_servers = ['kafka-broker-1:9092'],  value_serializer = lambda x:dumps(x).encode('utf-8'))

# for n in range(200, 250):
while True:
    n = random.choice([1, 2, 3, 4, 5, 6, 7, 8, 9, 0])
    my_data = {'data' : str(n)}
    print(my_data)
    my_producer.send(topic = 'test-topic-2', value = my_data)
    sleep(10)

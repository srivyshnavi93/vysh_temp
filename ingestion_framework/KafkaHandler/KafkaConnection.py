from ingestion_framework.Includes.Utils import try_get_column_str, get_secret
from pyspark.sql import Row
from typing import Optional
from dataclasses import dataclass

@dataclass
class KafkaConnection:
    topic:str
    username:str 
    password:str
    server:str
    region:str
    minPartitions:Optional[str]
    maxOffsetsPerTrigger:Optional[str]

    @classmethod     
    def from_row(cls, row: Row): 
    
        metadata_server = row["server"]
        region=row["region"]

        # kafka_server = get_secret(row["server"])
        # username = get_secret(row["username_key"]),
        # password = get_secret(row["password_key"]),

        kafka_server = 'kafka-broker:9092'
        username = None
        password = None

        topic = row["kafka_topic"]

        if kafka_server is None:
            raise Exception(f"kafka_server {metadata_server} null for topic: {topic}")
        
        return cls(topic, 
                username,
                password,
                kafka_server,
                region,                
                minPartitions=try_get_column_str(row, 'minPartitions'),
                maxOffsetsPerTrigger=try_get_column_str(row, 'maxOffsetsPerTrigger'),
                )
        
    def get_sal_jaas_config_str(self): 
        return f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{self.username}' password='{self.password}';"
    
    def get_servers(self):
        return f"{self.server}"
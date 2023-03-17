from ingestion_framework.Includes.Utils import get_trigger_interval, get_max_offsets_per_trigger
from ingestion_framework.KafkaHandler.KafkaConnection import KafkaConnection
from ingestion_framework.KafkaHandler.BronzeTarget import BronzeTarget
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DateType
from pyspark.sql.streaming import StreamingQuery
from dateutil import parser
import certifi
from confluent_kafka import Consumer, TopicPartition
import time

class KafkaHandler: 

    def __init__(self, spark_session:SparkSession, kc:KafkaConnection):

        self.spark = spark_session
        self.kc = kc
        self.streaming_query_name = f"{self.kc.topic}"+f"_{self.kc.region}_stream"
        log4j = self.spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger(__name__)
        
    def get_stream_options(self):

        topic = self.kc.topic
        stream_options = {
            "kafka.bootstrap.servers": self.kc.get_servers(),
            # "kafka.security.protocol": "SASL_SSL",
            # "kafka.sasl.mechanism": "PLAIN",
            # "kafka.sasl.jaas.config": self.kc.get_sal_jaas_config_str(),
            "subscribe": topic,
            "failOnDataLoss": "false",
            "includeHeaders": "true",
            "startingOffsets": "earliest"
        }

        if self.kc.minPartitions:
            stream_options["minPartitions"] = self.kc.minPartitions

        if self.kc.maxOffsetsPerTrigger:
            stream_options["maxOffsetsPerTrigger"] = get_max_offsets_per_trigger(self.kc.maxOffsetsPerTrigger)          

        return stream_options
        
    def read(self) -> DataFrame:

        stream_options = self.get_stream_options()
            
        return self.spark.readStream \
            .format("kafka") \
            .options(**stream_options) \
            .load()
    
    def write_bronze(self, table: BronzeTarget) -> StreamingQuery:

        ckpt_loc = table.streaming_ckpt
        
        self.spark.sql(table.create_bronze_table(topic_name=self.kc.topic))

        df = self.read()
        df = df.withColumn("kafka_date", col("timestamp").cast(DateType()))
        
        write_stream = df.writeStream
        
        stream_query = write_stream.option("checkpointLocation", ckpt_loc)
        
        trigger_interval = get_trigger_interval(table.trigger_interval)
        
        if trigger_interval.lower() == 'availablenow':
            stream_query = stream_query.trigger(availableNow=True)
        else:
            stream_query = stream_query.trigger(processingTime=trigger_interval)
        
        stream_query = (
                     stream_query 
                     .queryName(self.streaming_query_name)
                     .format("delta")
                     .outputMode('append')
                     .foreachBatch(lambda df, batchId: self.foreach_batch_function(table, df, batchId, stream_query))
                     .start()
                    )

        return stream_query
    
    
    def foreach_batch_function(self, table, df, batchId, stream_query):

        #TODO: Use asyncio to execute these functions asynchronously
        table.write_to_bronze(df, batchId, self.logger, self.spark)
        self.commit_offsets(stream_query)


    def commit_offsets(self, stream_query: StreamingQuery): 

        logger = self.logger     

        try:
            kc = self.kc
            streaming_query_name = self.streaming_query_name
            last_progress = stream_query.lastProgress

            if last_progress is not None:

                logger.info(f"{streaming_query_name} last progress: {last_progress}")
                last_progress_sources = last_progress["sources"]
                kafka_topic = kc.topic

                if last_progress_sources is not None:

                    for source in last_progress_sources:
                        end_offsets = source["endOffset"]
                        offsets_to_commit = [TopicPartition(kafka_topic, int(part), int(off)) for (part, off) in end_offsets[kafka_topic].items()]

                    group_id = f"{kafka_topic}-lakehouse-ingestion-worker"

                    conf = {
                        'bootstrap.servers': kc.server,
                        'group.id': group_id,
                        # 'security.protocol': 'SASL_SSL',
                        # 'enable.ssl.certificate.verification': False,
                        'enable.auto.commit': False,
                        # 'sasl.mechanism': 'PLAIN',
                        # 'sasl.username': kc.username,
                        # 'sasl.password': kc.password,
                        'session.timeout.ms': 2000,
                        'socket.connection.setup.timeout.ms': 1500,
                        'socket.timeout.ms': 1500,
                        'reconnect.backoff.max.ms': 200,
                        'ssl.ca.location': certifi.where()
                    }
                    consumer = Consumer(conf)

                    event_date_str = last_progress["timestamp"]
                    event_timestamp = parser.parse(event_date_str).timestamp()
                    current_time = time.time()
                    time_since_event = current_time - event_timestamp
                    
                    try:
                        consumer.commit(offsets = offsets_to_commit, asynchronous = True)
                        logger.info(f"************ {streaming_query_name} committing offsets (time since event {time_since_event}): {offsets_to_commit} ***********")
                    except Exception as e:
                        logger.error(f"{streaming_query_name} - error commiting offsets: {e}")
                    finally:
                        consumer.close() 
                    
        except Exception as e:
            logger.warn(e)       
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import json
from KafkaHandler.KafkaHandler import KafkaHandler
from KafkaHandler.KafkaConnection import KafkaConnection
from KafkaHandler.BronzeTarget import BronzeTarget

class KafkaToBronze:
    
    def run_streams(spark: SparkSession, module_parameters_str):
        log4j = spark._jvm.org.apache.log4j
        logger = log4j.LogManager.getLogger(__name__)
        logger.info(f"module_parameters_str: {module_parameters_str}")

        module_parameters = json.loads(module_parameters_str[1:-1])

        metadata_table = 'bronze.metadata_table'
        pipeline_id = None
        group_id = None

        logger.info(f"module_parameters: {module_parameters}")
        
        for p in module_parameters:
            if p == 'group_id':
                group_id = module_parameters['group_id']
            if p == 'metadata_table':
                metadata_table = module_parameters['metadata_table']
            if p == 'pipeline_id':
                pipeline_id = module_parameters['pipeline_id']

        ## group_id is a required parameter. Raise a RuntimeError if not supplied
        if group_id is None:
            raise RuntimeError('group_id is a required parameter for the KafkaToBronze module')                
        
        if pipeline_id is not None:
            pipeline_rows = spark.table(metadata_table).where(col('group_id') == group_id).where(col('pipeline_id') == pipeline_id).collect()
        else:
            pipeline_rows = spark.table(metadata_table).where(col('group_id') == group_id).collect()
            
        assert len(pipeline_rows) > 0, 'No pipelines to run!'

        pipelines = [(KafkaHandler(spark_session=spark, kc=KafkaConnection.from_row(r)), BronzeTarget.from_row(r), r) for r in pipeline_rows]

        assert len(pipelines) > 0, 'No pipelines to run!'

        for kafka_handler, bronze_target, r in pipelines: 
            stream = kafka_handler.write_bronze(table=bronze_target)
        spark.streams.awaitAnyTermination()
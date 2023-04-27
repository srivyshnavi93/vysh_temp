import findspark 
findspark.init() 

import pyspark
from delta import *
import pandas as pd
from pyspark.sql.functions import col

import sys
sys.path.append('/opt/spark/work-dir/odp_intra_storage/ingestion_framework/')
from spark_ingestion_app.spark_ingestion_app import SparkIngestionApp

spark = pyspark.sql.SparkSession.builder.master("spark://spark-master:7077").appName("spark_kafka").config("spark.sql.streaming.metricsEnabled", "true").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('INFO')

module_parameters = '\'{"group_id": "1", "pipeline_id": "1"}\''

SparkIngestionApp.run_module(spark = spark, module_name = 'KafkaToBronze', module_parameters = module_parameters, log_level='INFO')
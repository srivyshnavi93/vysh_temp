import findspark 
findspark.init() 

import pyspark
from delta import *
import pandas as pd
from pyspark.sql.functions import col

spark = pyspark.sql.SparkSession.builder.master("spark://spark-master:7077").config("spark.cores.max", "1").appName("bronze_silver").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('INFO')

import sys
sys.path.append('/opt/spark/work-dir/odp_intra_storage/vysh_temp/ingestion_framework/')

from spark_ingestion_app.spark_ingestion_app import SparkIngestionApp

spark.sql("""
create table if not exists silver.weather_v1
(
message_key string,
load_time timestamp,
kafka_topic string,
kafka_partition int,
kafka_offset bigint,
kafka_timestamp timestamp,
kafka_date date,
location string,
time string,
info string,
temperature string,
precipitation string,
humidiy string,
wind string
)
using delta
""")

module_parameters = '\'{"group_id": "1", "pipeline_id": "1"}\''

SparkIngestionApp.run_module(spark = spark, module_name = 'BronzeToSilver', module_parameters = module_parameters, log_level='INFO')

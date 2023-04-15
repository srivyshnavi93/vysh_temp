import argparse
import sys
import os

from KafkaHandler.KafkaToBronze import KafkaToBronze
from SilverTarget.BronzeToSilver import BronzeToSilver
from pyspark.sql import SparkSession


class SparkIngestionApp:
    """
        Python class used as an entry point for Spark to run ingestion modules.

        The module name passed as argument will map to the python module that needs to run for the job.
        The module parameters are a dictionary type with the parameters required to run the specified module.
        """
    
    def run_module(spark: SparkSession, module_name, module_parameters, log_level='INFO'):

        if module_name == 'KafkaToBronze':
            KafkaToBronze.run_streams(spark, module_parameters)
        elif module_name == 'BronzeToSilver':
            BronzeToSilver.run_streams(spark, module_parameters)

if __name__ == '__main__':

    if os.path.exists('ingestion_framework.zip'):
        sys.path.insert(0, 'ingestion_framework.zip')
    else:
        sys.path.insert(0, './ingestion_framework')       
        
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=(argparse.RawDescriptionHelpFormatter)
    )
    parser.add_argument(
        "--module_name", 
        help="Name of the python module to run.", 
        choices=['LayerOptimizer', 'KafkaToBronze', 'BronzeToSF', 'BronzeToSFCustom'],
        required=True
        )
    parser.add_argument(
        "--module_parameters", 
        help="Dictionary of parameters to use in the python module.", 
        required=True
        )
    parser.add_argument(
        "--log_level",
        help='Log level to use for the spark context.  Should be INFO, WARN, or ERROR.',
        choices=['INFO','WARN','ERROR'],
        required=True
    )

    args = parser.parse_args()

    spark = SparkSession.builder.master("local[*]").appName(args.module_name).getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel(args.log_level) 

    SparkIngestionApp.run_module(spark, args.module_name, args.module_parameters, args.log_level.upper())

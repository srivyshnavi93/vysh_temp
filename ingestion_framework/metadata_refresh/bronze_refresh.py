import argparse
from pyspark.sql.functions import regexp_replace
from pyspark.sql import SparkSession

def refresh_bronze_metadata(spark: SparkSession, env: str, file_location: str, table_name: str):

    new_metadata = (
        spark.read.csv(file_location, header = True)
        .withColumn("kafka_topic", regexp_replace("kafka_topic", '\{env\}', env))
        .withColumn("bronze_table", regexp_replace("bronze_table", '\{env\}', env))
        .withColumn("bronze_checkpoint", regexp_replace("bronze_checkpoint", '\{env\}', env))
    )

    assert(new_metadata.count() == new_metadata.select("pipeline_id").distinct().count())
    assert(new_metadata.count() == new_metadata.select("kafka_topic", "region").distinct().count())
    assert(new_metadata.count() == new_metadata.select("bronze_table", "region").distinct().count())
    assert(new_metadata.count() == new_metadata.select("bronze_checkpoint").distinct().count())

    new_metadata.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

if __name__=='__main__':
    
    spark = SparkSession.builder.master("local[*]").appName("bronze_metadata_refresh").getOrCreate()
    
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=(argparse.RawDescriptionHelpFormatter)
    )
    
    parser.add_argument(
        '--env',
        help='Build environment',
        choices=['stg', 'prd', 'test'],
        required=False,
        default='test'
    )
    parser.add_argument(
        '--file_location',
        help='location for metadata csv file',
        required=True,
    )
    parser.add_argument(
        '--table_name',
        help='metadata table name, including database name (i.e. bronze.metadata_table)',
        required=False,
        default='bronze.metadata_table'
    )

    args = parser.parse_args()

    refresh_bronze_metadata(
        spark,
        args.env,
        args.file_location,
        args.table_name
    )
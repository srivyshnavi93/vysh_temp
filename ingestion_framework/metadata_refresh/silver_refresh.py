import argparse
from pyspark.sql.functions import regexp_replace
from pyspark.sql import SparkSession

def refresh_silver_metadata(spark: SparkSession, env: str, file_location: str, table_name: str):

    new_metadata = (
        spark.read.csv(file_location, header = True)
        .withColumn("bronze_table", regexp_replace("bronze_table", '\{env\}', env))
        .withColumn("silver_table", regexp_replace("silver_table", '\{env\}', env))
        .withColumn("silver_checkpoint", regexp_replace("silver_checkpoint", '\{env\}', env))
        .withColumn("schema_json_loc", regexp_replace("schema_json_loc", '\{env\}', env))
        .withColumn("silver_transformation_json_path", regexp_replace("silver_transformation_json_path", '\{env\}', env))
        .withColumn("silver_custom_merge_path", regexp_replace("silver_custom_merge_path", '\{env\}', env)) \
    )

    assert(new_metadata.count() == new_metadata.select("pipeline_id").distinct().count())
    assert(new_metadata.count() == new_metadata.select("silver_table").distinct().count())
    assert(new_metadata.count() == new_metadata.select("silver_checkpoint").distinct().count())

    new_metadata.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

if __name__=='__main__':

    spark = SparkSession.builder.master("local[*]").appName("silver_metadata_refresh").getOrCreate()    

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
        help='metadata table name, including database name (i.e. silver.metadata_table)',
        required=False,
        default='silver.metadata_table'
    )

    args = parser.parse_args()

    refresh_silver_metadata(
        spark,        
        args.env,
        args.file_location,
        args.table_name
    )
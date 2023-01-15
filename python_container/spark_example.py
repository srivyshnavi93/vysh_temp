import pyspark
spark = pyspark.sql.SparkSession.builder.appName("spark-sample").getOrCreate()
sample = spark.range(1000 * 1000 * 1000).count()
print(sample)
import pyspark
# spark = pyspark.sql.SparkSession.builder.master("spark://127.0.0.1:7077").appName("spark-sample").getOrCreate()
spark = pyspark.sql.SparkSession.builder.appName("spark-sample").getOrCreate()
sample = spark.range(10000000 + 1).count()
print(sample)
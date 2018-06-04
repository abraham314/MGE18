
from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName('cruise').getOrCreate()

# read csv
df = spark.read.csv("s3a://tar7/all_data.csv",inferSchema=True,header=True)


#df.show()

df.write.parquet("s3a://tar7/proto.parquet")


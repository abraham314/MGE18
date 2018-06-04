from pyspark import SparkContext
from pyspark.sql import *    
import sys
if __name__ == "__main__":
    sc = SparkContext(appName="Parquet")
    spark = SQLContext(sc)

    s3_path = str(sys.argv[1])

    profeco = spark.read.csv(s3_path+'/profeco/profeco.csv', header =True)     
    profeco.write.format('parquet').option("compression", "gzip").save(s3_path+"/profeco/profeco_gzip", mode='OVERWRITE')
    sc.stop()
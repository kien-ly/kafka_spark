from pyspark.sql import SparkSession
#  init spark session to connect  postgresql 
spark = SparkSession \
         .builder \
         .appName("PySpark App") \
         .config("spark.jars", "postgresql-42.3.3.jar") \
         .getOrCreate()

df=spark.read.format("csv").load("csv00000175.csv")# load csv using spark

mode = "overwrite"
url = "jdbc:postgresql://localhost:5432/drone-log"
properties = {"user": "postgres","password": "1234","driver": "org.postgresql.Driver"}

df.write.jdbc(url=url, table="log_drone_spark3", mode=mode, properties=properties)
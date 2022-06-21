import json
import requests
from bson import json_util
from dateutil import parser
from pyspark import SparkContext
from kafka import KafkaConsumer, KafkaProducer
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#Mongo DB
# from pymongo import MongoClient
# client = MongoClient('localhost', 27017)
# db = client['RealTimeDB']
# collection = db['RealTimeCollection']

kafka_topic_name = "test-topic"
kafka_bootstrap_servers = 'localhost:9092'


if __name__ == "__main__":
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as CSV") \
        .master("local[*]") \
        .config('spark.driver.host', '127.0.0.2') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0') \
        .getOrCreate()

        #scala version 2.12 --> spark-sql-kafka-0-10_2.12:3.2.0
        #Spark version is 3.0.1 so I have :3.0.1
        
    # spark.sparkContext.setLogLevel("WARN")

    # sc=spark.sparkContext.getOrCreate()
    # sc.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("WARN")


    
def structure_validate_data(msg):    
    
    data_dict={}
    
    #create RDD
    rdd=spark.parallelize(msg.value.decode("utf-8").split())
    # rdd=sc.parallelize(msg.value.decode("utf-8").split())
    
    data_dict["test-topic"]=str(msg.value.decode("utf-8"))
    
    #data validation and create json data dict
    try:
        data_dict["TimeStamp"]=parser.isoparse(rdd.collect()[0])
        
    except Exception as error:
        
        
        data_dict["TimeStamp"]="Error"
    
    try:
        data_dict["WaterTemperature"]=float(rdd.collect()[1])
        
        if (((data_dict["WaterTemperature"])>99) | ((data_dict["WaterTemperature"])<-10)):
            
            data_dict["WaterTemperature"]="Sensor Malfunctions"
        
        
    except Exception as error:
        
        
        data_dict["WaterTemperature"]="Error"
        
        
    try:
        data_dict["Turbidity"]=float(rdd.collect()[2])
        
        if (((data_dict["Turbidity"])>5000)):
            
            data_dict["Turbidity"]="Sensor Malfunctions"
        
        
    except Exception as error:
        
        
        data_dict["Turbidity"]="Error"      
    
        
    try:
        data_dict["BatteryLife"]=float(rdd.collect()[3])
        
    except Exception as error:
        
        data_dict["BatteryLife"]="Error"
    
    
    try:
        data_dict["Beach"]=str(rdd.collect()[4])
        
    except Exception as error:
            
        data_dict["Beach"]="Error"
        
    try:
        data_dict["MeasurementID"]=int(str(rdd.collect()[5]).replace("Beach",""))
        
    except Exception as error:
        
        data_dict["MeasurementID"]="Error"

    
    
    return data_dict



consumer = KafkaConsumer('test-topic', auto_offset_reset='earliest',bootstrap_servers=['localhost:9092'], consumer_timeout_ms=10000)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

for msg in consumer:
    if msg.value.decode("utf-8")!="Error in Connection":
        data=structure_validate_data(msg)
        producer.send("CleanSensorData", json.dumps(data, default=json_util.default).encode('utf-8'))
        time.sleep(10)
        # if timestamp_exist(data['TimeStamp'])==False:            
        #     #push data to mongo db
        #     collection.insert(data)
        #     producer.send("CleanSensorData", json.dumps(data, default=json_util.default).encode('utf-8'))
        
        print(data)


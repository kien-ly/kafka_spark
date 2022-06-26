import json
from os import truncate
import requests
from bson import json_util
from dateutil import parser
from pyspark import SparkContext
from kafka import KafkaConsumer, KafkaProducer
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd

kafka_topic_name = "RawData"
kafka_bootstrap_servers = 'localhost:9092'

def createDF(msg):
    rdd=sc.parallelize(msg.value.decode("utf-8").split())
    li=list(rdd.collect())
    return li

def structure_validate_data(msg):    
    
    data_dict={}    
    #create RDD
    rdd=sc.parallelize(msg.value.decode("utf-8").split( ))
    
    # data_dict[kafka_topic_name]=str(msg.value.decode("utf-8"))
    
    #data validation and create json data dict
    try:
        # data_dict["TimeStamp"]=parser.isoparse(rdd.collect()[0])
        data_dict["TimeStamp"]=str(rdd.collect()[0])
        
    except Exception as error:
        data_dict["TimeStamp"]="Error"
        
    
    try:
        data_dict["Beach"]=str(rdd.collect()[1])
        
    except Exception as error:            
        data_dict["Beach"]="Error"
        
        
    try:
        data_dict["WaterTemperature"]=float(str(rdd.collect()[2]).replace("Beach",""))
        
        if (((data_dict["WaterTemperature"])>99) | ((data_dict["WaterTemperature"])<-10)):
            
            data_dict["WaterTemperature"]="Sensor Malfunctions"     
        
    except Exception as error:            
        data_dict["WaterTemperature"]="Error"
        
        
    try:
        data_dict["Turbidity"]=float(rdd.collect()[3])        
        if (((data_dict["Turbidity"])>5000)):            
            data_dict["Turbidity"]="Sensor Malfunctions"     
        
    except Exception as error:                
        data_dict["Turbidity"]="Error"      
    
        
    try:
        data_dict["BatteryLife"]=float(rdd.collect()[4])        
    except Exception as error:        
        data_dict["BatteryLife"]="Error"
        
        
    try:
        data_dict["MeasurementID"]=int(rdd.collect()[5])
        
    except Exception as error:        
        data_dict["MeasurementID"]="Error"    
    
    return data_dict


consumer = KafkaConsumer(kafka_topic_name, auto_offset_reset='earliest',bootstrap_servers=[kafka_bootstrap_servers], consumer_timeout_ms=10000)

# producer = KafkaProducer(bootstrap_servers=[kafka_bootstrap_servers])

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
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    list_data=[]
    sub = ['timestamp','beach','water_temperature','turbidity','battery_life','measurement_id']
    for msg in consumer:
        if msg.value.decode("utf-8")!="Error in Connection":
            data=structure_validate_data(msg)
            data2=createDF(msg)
            # pdf=pd.DataFrame([data])            
            list_data.append(data2)            
            # time.sleep(0.01)
    pdf=pd.DataFrame(list_data,columns=sub)
    df=spark.createDataFrame(pdf)
    print(pdf)
    df.show()
    df.write.mode('append').parquet('/home/tkien/project/kafka_project/data/data.parquet')
    # df.write.mode('overwrite').parquet('/home/tkien/project/kafka_project/data/data.parquet')

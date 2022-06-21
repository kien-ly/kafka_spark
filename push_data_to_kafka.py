import time
import json
import requests
import datetime
# from kafka import KafkaProducer, KafkaClient
from kafka import KafkaProducer
# from websocket import create_connection

#pip install kafka-python
KAFKA_TOPIC_NAME_CONS="test-topic"
# KAFKA_TOPIC_NAME_CONS="RawData"
KAFKA_BOOTSTRAP_SERVERS_CONS='localhost:9092'

def get_sensor_data_stream():
    try:
        url = 'http://0.0.0.0:3030/sensordata'
        r = requests.get(url)
        return r.text
    except:
        return "Error in Connection"


# producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                    value_serializer=lambda x: x.encode('utf-8'),
                                    api_version=(0, 11, 5))
while True:
    msg =  get_sensor_data_stream()
    # producer.send("RawSensorData", msg.encode('utf-8'))
    producer.send(KAFKA_TOPIC_NAME_CONS,msg)
    time.sleep(10)

# if __name__ =="__main__":

    


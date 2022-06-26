
import time
import random
from datetime import datetime
from flask import Flask, Response

app = Flask(__name__)

#data
@app.route('/sensordata')
def get_sensor_data():
    beach='Vung_Tau'
    message_fields_value_list=[]
    timestamp="{}".format((datetime.now()).now().isoformat())
    water_temperature=str(round(random.uniform(31.5, 0.0),2))
    turbidity=str(round(random.uniform(1683.48, 0.0),2))
    battery_life=str(round(random.uniform(13.3,4.8),2))
    measurement_id=str(random.randint(10000,999999))
    
    message_fields_value_list.append(timestamp)
    message_fields_value_list.append(beach)
    message_fields_value_list.append(water_temperature)
    message_fields_value_list.append(turbidity)
    message_fields_value_list.append(battery_life)
    message_fields_value_list.append(measurement_id)    
    response=" ".join(message_fields_value_list)
    print("Message Type:",type(response))
    # response=str(timestamp+" "+water_temperature+" "+turbidity+" "+battery_life+" "+beach+" "+measurement_id)
    print(response)
    return Response(response, mimetype='text/plain')

if __name__ == '__main__':
    app.run(host='127.0.0.1',port='3030')

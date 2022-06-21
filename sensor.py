
import time
import random
from datetime import datetime
from flask import Flask, Response

app = Flask(__name__)

#data
@app.route('/sensordata')
def get_sensor_data():
    beach='Vung_Tau'

    timestamp="{}".format((datetime.now()).now().isoformat())
    water_temperature=str(round(random.uniform(31.5, 0.0),2))
    turbidity=str(round(random.uniform(1683.48, 0.0),2))
    battery_life=str(round(random.uniform(13.3,4.8),2))
    measurement_id=str(random.randint(10000,999999))
    
    response=str(timestamp+" "+water_temperature+" "+turbidity+" "+battery_life+" "+beach+" "+measurement_id)
    print(response)
    return Response(response, mimetype='text/plain')

if __name__ == '__main__':
    app.run(host='127.0.0.1',port='3030')

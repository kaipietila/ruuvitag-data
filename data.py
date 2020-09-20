from settings import db_settings

from influxdb import InfluxDBClient
from ruuvitag_sensor.ruuvi import RuuviTagSensor


client = InfluxDBClient(host=db_settings.HOST, port=db_settings.PORT, database=db_settings.DATABASE_NAME)

def write_to_influxdb(received_data):

    mac = received_data[0]
    payload = received_data[1]
    if ('data_format' in payload):
        data_format = payload['data_format']

    fields = {}
    if ('temperature' in payload):
        fields['temperature'] = payload['temperature'] 
    if ('humidity' in payload):
        fields['humidity'] = payload['humidity'] 
    if ('pressure' in payload):
        fields['pressure'] = payload['pressure'] 
    if ('acceleration_x' in payload):
        fields['acceleration_x'] = payload['acceleration_x']
    if ('acceleration_y' in payload):
        fields['acceleration_y'] = payload['acceleration_y'] 
    if ('acceleration_z' in payload):
        fields['acceleration_z'] = payload['acceleration_z'] 
    if ('battery' in payload):
        fields['battery_voltage'] = payload['battery']/1000.0
    if ('tx_power' in payload):
        fields['tx_power'] = payload['tx_power']
    if ('movement_counter' in payload):
        fields['movement_counter'] = payload['movement_counter']
    if ('measurement_sequence_number' in payload):  
        fields['measurement_sequence_number'] = payload['measurement_sequence_number'] 
    if ('tagID' in payload):   
        fields['tag_iD'] = payload['tagID']
    if ('rssi' in payload):
        fields['rssi'] = payload['rssi'] 
    json_body = [
        {
            'measurement': 'ruuvi_measurements',
            'tags': {
                'mac': mac,
                'dataFormat': data_format
            },
            'fields': fields
        }
    ]
    client.write_points(json_body)

def get_data_and_write():
    RuuviTagSensor.get_datas(write_to_influxdb)

if if __name__ == "__main__":
    get_data_and_write()

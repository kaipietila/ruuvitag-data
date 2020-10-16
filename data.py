from datetime import datetime
from influxdb import InfluxDBClient
from ruuvitag_sensor.ruuvi import RuuviTagSensor
from ruuvitag_sensor.ruuvi import RunFlag


client = InfluxDBClient(host='localhost', port=8086, database='ruuvi')

run_flag = RunFlag()

macs = {}
mac_list = []

start_time = ''

def can_write_once_per_minute(mac):
    """
    Restricts writes to the db to once a minute if data is not required
    to be more frequent
    """
    global macs
    time_now = datetime.now()
    if macs.get(mac):
        time_elasped = time_now - macs[mac]['last_write_time']
        if time_elasped.seconds > 59:
            macs[mac]['last_write_time'] = time_now
            return True
        else:
            return False
    else:
        macs[mac] = {}
        macs[mac]['last_write_time'] = time_now
        return True
    
def can_write_once_per_mac(mac):
    """
    One write per mac per process. Meant to be used as cronjob every x minutes
    """
    global mac_list
    if mac in mac_list and len(mac_list) == 3:
        run_flag.running = False
        return False
    elif mac in mac_list:
        return False
    else:
        mac_list.append(mac)
        return True

def handle_data_not_being_received()
    global start_time
    time_elasped = datetime.now() - start_time
    if time_elasped.seconds > 59:
        run_flag.running = False
    
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
    print(f'Processed data for {mac} at {datetime.now()}')

def handle_data(received_data):
    """
    Checks that data is not written too frequently. Otherwise just returns
    """
    mac = received_data[0]
    if can_write_once_per_mac(mac):
        write_to_influxdb(received_data)
    handle_data_not_being_received()

def get_data_and_write():
    RuuviTagSensor.get_datas(handle_data, run_flag=run_flag)

if __name__ == "__main__":
    global start_time
    start_time = datetime.now()
    get_data_and_write()

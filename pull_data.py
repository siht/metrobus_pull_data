import json
from urllib.request import urlopen
import pandas as pd
from sqlalchemy import create_engine

from config import (
    DATABASES,
    METROBUSES_API_URL,
)
from utils import construct_engine_string

metrobus_database_conf = DATABASES['metrobuses']

metrobus_engine_string = construct_engine_string(metrobus_database_conf)

# engine = create_engine(metrobus_engine_string)

# metrobuses_raw_data = urlopen(METROBUSES_API_URL).read()
metrobuses_raw_data = b'{"nhits": 207, "parameters": {"dataset": "prueba_fetchdata_metrobus", "timezone": "UTC", "rows": 10, "format": "json"}, "records": [{"datasetid": "prueba_fetchdata_metrobus", "recordid": "64868a42d9ed94e2e979cf16603e2fde9afbfe16", "fields": {"vehicle_id": "170", "date_updated": "2020-05-25 12:00:04", "position_longitude": -99.18779754638672, "trip_schedule_relationship": 2, "position_speed": 16, "position_latitude": 19.3174991607666, "trip_route_id": "367", "vehicle_label": "112", "position_odometer": 231, "vehicle_current_status": 2, "geographic_point": [19.317499160767, -99.187797546387]}, "geometry": {"type": "Point", "coordinates": [-99.187797546387, 19.317499160767]}, "record_timestamp": "2020-05-25T17:00:05.715000+00:00"}, {"datasetid": "prueba_fetchdata_metrobus", "recordid": "982dfb9f7e4c96be8f0a233344baddcd418c7737", "fields": {"vehicle_id": "177", "trip_start_date": "20200428", "date_updated": "2020-05-25 12:00:04", "position_longitude": -99.17749786376953, "trip_schedule_relationship": 0, "position_speed": 13, "position_latitude": 19.292600631713867, "trip_route_id": "367", "vehicle_label": "119", "position_odometer": 0, "trip_id": "9732304", "vehicle_current_status": 1, "geographic_point": [19.292600631714, -99.17749786377]}, "geometry": {"type": "Point", "coordinates": [-99.17749786377, 19.292600631714]}, "record_timestamp": "2020-05-25T17:00:05.715000+00:00"}, {"datasetid": "prueba_fetchdata_metrobus", "recordid": "f384182d403eda6bac4804f92a7259ff8a865587", "fields": {"vehicle_id": "178", "trip_start_date": "20200428", "date_updated": "2020-05-25 12:00:04", "position_longitude": -99.1791000366211, "trip_schedule_relationship": 0, "position_speed": 0, "position_latitude": 19.37339973449707, "trip_route_id": "367", "vehicle_label": "120", "position_odometer": 510, "trip_id": "9731964", "vehicle_current_status": 2, "geographic_point": [19.373399734497, -99.179100036621]}, "geometry": {"type": "Point", "coordinates": [-99.179100036621, 19.373399734497]}, "record_timestamp": "2020-05-25T17:00:05.715000+00:00"}, {"datasetid": "prueba_fetchdata_metrobus", "recordid": "9b697a95fd3bdd9358db5a1b5692453b5e60869e", "fields": {"vehicle_id": "1286", "trip_start_date": "20200428", "date_updated": "2020-05-25 12:00:04", "position_longitude": -99.11009979248047, "trip_schedule_relationship": 0, "position_speed": 5, "position_latitude": 19.45009994506836, "trip_route_id": "161", "vehicle_label": "219", "position_odometer": 0, "trip_id": "9678929", "vehicle_current_status": 2, "geographic_point": [19.450099945068, -99.11009979248]}, "geometry": {"type": "Point", "coordinates": [-99.11009979248, 19.450099945068]}, "record_timestamp": "2020-05-25T17:00:05.715000+00:00"}, {"datasetid": "prueba_fetchdata_metrobus", "recordid": "b38f4b2baecc159a3ef89b38ec340be0589153ef", "fields": {"vehicle_id": "1288", "date_updated": "2020-05-25 12:00:04", "position_longitude": -99.04730224609375, "trip_schedule_relationship": 2, "position_speed": 0, "position_latitude": 19.39080047607422, "vehicle_label": "221", "position_odometer": 46, "vehicle_current_status": 2, "geographic_point": [19.390800476074, -99.047302246094]}, "geometry": {"type": "Point", "coordinates": [-99.047302246094, 19.390800476074]}, "record_timestamp": "2020-05-25T17:00:05.715000+00:00"}, {"datasetid": "prueba_fetchdata_metrobus", "recordid": "b4657b45122b9990fb811319331dc94e963349db", "fields": {"vehicle_id": "367", "trip_start_date": "20200428", "date_updated": "2020-05-25 12:00:04", "position_longitude": -99.06159973144531, "trip_schedule_relationship": 0, "position_speed": 8, "position_latitude": 19.38279914855957, "trip_route_id": "301", "vehicle_label": "309", "position_odometer": 114, "trip_id": "9737575", "vehicle_current_status": 2, "geographic_point": [19.38279914856, -99.061599731445]}, "geometry": {"type": "Point", "coordinates": [-99.061599731445, 19.38279914856]}, "record_timestamp": "2020-05-25T17:00:05.715000+00:00"}, {"datasetid": "prueba_fetchdata_metrobus", "recordid": "0bd2e8e281e10fac7ed5358fb666dc004a68f68b", "fields": {"vehicle_id": "432", "trip_start_date": "20200428", "date_updated": "2020-05-25 12:00:04", "position_longitude": -99.18710327148438, "trip_schedule_relationship": 0, "position_speed": 0, "position_latitude": 19.402000427246094, "trip_route_id": "301", "vehicle_label": "374", "position_odometer": 0, "trip_id": "9736405", "vehicle_current_status": 1, "geographic_point": [19.402000427246, -99.187103271484]}, "geometry": {"type": "Point", "coordinates": [-99.187103271484, 19.402000427246]}, "record_timestamp": "2020-05-25T17:00:05.715000+00:00"}, {"datasetid": "prueba_fetchdata_metrobus", "recordid": "389086052831d56f35906d30d4b9875cfea2e062", "fields": {"vehicle_id": "1021", "trip_start_date": "20200428", "date_updated": "2020-05-25 12:00:04", "position_longitude": -99.10289764404297, "trip_schedule_relationship": 0, "position_speed": 9, "position_latitude": 19.3971004486084, "trip_route_id": "301", "vehicle_label": "398", "position_odometer": 648, "trip_id": "9738355", "vehicle_current_status": 2, "geographic_point": [19.397100448608, -99.102897644043]}, "geometry": {"type": "Point", "coordinates": [-99.102897644043, 19.397100448608]}, "record_timestamp": "2020-05-25T17:00:05.715000+00:00"}, {"datasetid": "prueba_fetchdata_metrobus", "recordid": "a673f4649b59b8354a093da3ead2c97de4db8626", "fields": {"vehicle_id": "22", "trip_start_date": "20200428", "date_updated": "2020-05-25 12:00:04", "position_longitude": -99.15409851074219, "trip_schedule_relationship": 0, "position_speed": 1, "position_latitude": 19.419599533081055, "trip_route_id": "1", "vehicle_label": "422", "position_odometer": 560, "trip_id": "9685769", "vehicle_current_status": 2, "geographic_point": [19.419599533081, -99.154098510742]}, "geometry": {"type": "Point", "coordinates": [-99.154098510742, 19.419599533081]}, "record_timestamp": "2020-05-25T17:00:05.715000+00:00"}, {"datasetid": "prueba_fetchdata_metrobus", "recordid": "64d6b5427412975a536192f8d917e558a7ce39c1", "fields": {"vehicle_id": "23", "trip_start_date": "20200428", "date_updated": "2020-05-25 12:00:04", "position_longitude": -99.14800262451172, "trip_schedule_relationship": 0, "position_speed": 5, "position_latitude": 19.47920036315918, "trip_route_id": "1", "vehicle_label": "423", "position_odometer": 561, "trip_id": "9685204", "vehicle_current_status": 2, "geographic_point": [19.479200363159, -99.148002624512]}, "geometry": {"type": "Point", "coordinates": [-99.148002624512, 19.479200363159]}, "record_timestamp": "2020-05-25T17:00:05.715000+00:00"}]}'

metrobuses_data = json.loads(metrobuses_raw_data)

metrobuses_required_data = map(
    (lambda elem: {
        'serie': elem['fields']['vehicle_id'],
        'latitude': elem['fields']['position_latitude'],
        'longitude': elem['fields']['position_longitude'],
        'date_time': elem['fields']['date_updated']}
    ),
    metrobuses_data['records']
)

print(list(metrobuses_required_data))

# df = pd.read_sql_table('metrobus_history_district', engine)

# print(df)

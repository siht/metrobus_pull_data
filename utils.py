import decimal
import json

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from shapely.geometry import Polygon, Point

from config import DATABASES

__all__ = (
    'construct_engine_string',
    'create_historical_points',
    'filter_json_raw_data',
    'get_engine',
)


def construct_engine_string(db_config):
    '''
    recibe a dict and return a string
    '''
    return (
        "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        ).format(
            user=db_config['USER'],
            password=db_config['PASSWORD'],
            host=db_config['HOST'],
            port=db_config['PORT'],
            database=db_config['NAME'],
        )


def get_engine():
    '''
    return a preconfigured sqlalchemy engine
    '''
    metrobus_database_conf = DATABASES['metrobuses']
    metrobus_engine_string = construct_engine_string(metrobus_database_conf)
    return create_engine(metrobus_engine_string)


def filter_json_raw_data(data):
    '''
    return a dictionary with necesary values for update databas
   '''
    metrobuses_data = json.loads(data, parse_float=decimal.Decimal)

    return list(
        map(
            lambda elem: {
                'serie': elem['fields']['vehicle_id'],
                'latitude': elem['fields']['geographic_point'][0],
                'longitude': elem['fields']['geographic_point'][1],
                'date_time': elem['fields']['date_updated']
            },
            metrobuses_data['records']
        )
    )


def create_metrobuses_if_doesnt_exist(engine, data):
    '''
    create new metrobus registers in db if contains some new
    return a data frame with the new and registered registries
    with ids
    '''
    df_saved_metrobuses = (
        pd.read_sql_table('metrobus_history_metrobus', engine)
    )
    df_fetched_metrobuses = (
        pd.Series(map(lambda elem: elem['serie'], data)).to_frame('serie')
    )
    df_review_metrobuses = (
        df_saved_metrobuses
        .merge(df_fetched_metrobuses, on='serie', how='right')
    )
    df_new_metrobuses = (
        df_review_metrobuses[pd.isnull(df_review_metrobuses['id'])]
    )
    df_already_registered_metrobuses = (
        df_review_metrobuses.dropna(subset=['id'])
    )
    df_already_registered_metrobuses = (
        df_already_registered_metrobuses.set_index('id')
    )
    theres_new_metrobuses = not df_new_metrobuses.empty
    if(theres_new_metrobuses):
        last_id = (
            engine.execute(
                'SELECT id FROM metrobus_history_metrobus'
                ' ORDER BY id DESC LIMIT 1;'
            ).fetchone()
        )
        last_id = last_id[0] if last_id else 0
        current_id = last_id + 1
        next_new_id = current_id + len(df_new_metrobuses)
        new_ids = pd.Series(range(current_id, next_new_id))
        df_new_metrobuses['id'] = new_ids
        df_new_metrobuses = df_new_metrobuses.set_index('id')
        df_new_metrobuses.to_sql(
            'metrobus_history_metrobus',
            engine,
            if_exists='append'
        )
        return pd.concat(
            (df_already_registered_metrobuses, df_new_metrobuses,)
        )
    return df_already_registered_metrobuses


def create_places_if_doesnt_exist(engine, data):
    '''
    create new place registers in db if contains some new
    return a data frame with the new and registered registries
    with ids
    '''
    df_saved_places = (
        pd.read_sql(
            'SELECT id, latitude, longitude FROM metrobus_history_place;',
            engine,
            coerce_float=False
        )
    )
    df_fetched_places = (
        pd.DataFrame(
            map(
                (
                    lambda elem: {
                        'latitude': elem['latitude'],
                        'longitude': elem['longitude'],
                    }
                ),
                data
            ),
            dtype='object'
        )
    )
    df_review_places = (
        df_saved_places
        .merge(
            df_fetched_places,
            on=['latitude', 'longitude'],
            how='right'
        )
    )
    df_new_places = df_review_places[pd.isnull(df_review_places['id'])]
    df_already_registered_places = df_review_places.dropna(subset=['id'])
    df_already_registered_places = df_already_registered_places.set_index('id')
    theres_new_places = not df_new_places.empty
    if(theres_new_places):
        last_id = (
            engine.execute(
                'SELECT id FROM metrobus_history_place'
                ' ORDER BY id DESC LIMIT 1;'
            ).fetchone()
        )
        last_id = last_id[0] if last_id else 0
        current_id = last_id + 1
        next_new_id = current_id + len(df_new_places)
        new_ids = pd.Series(range(current_id, next_new_id))
        df_new_places['id'] = new_ids
        df_new_places = df_new_places.set_index('id')
        df_new_places['district_id'] = df_new_places.apply(
            get_district_id,
            axis=1
        )
        df_new_places.to_sql(
            'metrobus_history_place',
            engine,
            if_exists='append'
        )
        return pd.concat((df_new_places, df_already_registered_places,))
    return df_already_registered_places


def create_historical_points(engine, data):
    '''
    create new historical points, and create metrobuses registries
    and/or places registries if doesnt exist
    '''
    df_metrobuses = create_metrobuses_if_doesnt_exist(engine, data)
    df_metrobuses['metrobus_id'] = df_metrobuses.index
    df_places = create_places_if_doesnt_exist(engine, data)
    df_places['place_id'] = df_places.index
    df_origin_data = pd.DataFrame(data)

    df_origin_data = df_origin_data.merge(df_metrobuses, on='serie')
    df_origin_data = (
        df_origin_data.merge(df_places, on=['latitude', 'longitude'])
    )
    print(df_origin_data)
    df_origin_data.drop(
        ['serie', 'latitude', 'longitude'],
        axis=1,
        inplace=True
    )
    last_id_historical_point = (
        engine.execute(
            'SELECT id FROM metrobus_history_historicalpoint'
            ' ORDER BY id DESC LIMIT 1;'
        ).fetchone()
    )
    last_id_historical_point = (
        last_id_historical_point[0] if last_id_historical_point else 0
    )
    current_id_historical_point = last_id_historical_point + 1
    next_id_historical_point = (
        current_id_historical_point + len(df_origin_data)
    )
    new_ids = (
        pd.Series(
            range(current_id_historical_point, next_id_historical_point)
        )
    )
    df_origin_data['id'] = new_ids
    df_origin_data = df_origin_data.set_index('id')
    df_origin_data.to_sql(
        'metrobus_history_historicalpoint',
        engine,
        if_exists='append'
    )


def in_what_district_is_this_point(latitude, longitude, areas={}):
    '''
    get the id of mexico city districts based on latitude and
    longitude
    '''
    engine = get_engine()
    districts_with_id = (engine.execute(
        'SELECT name, id FROM metrobus_history_district;'
    ).fetchall())
    point_place = Point((latitude, longitude))
    for district_name, district_id in districts_with_id:
        if district_id not in areas:
            sql_consult = text(
                'SELECT latitude, longitude '
                'FROM metrobus_history_districtlimitpoints '
                'WHERE district_id = :id;'
            )
            areas[district_id] = Polygon(
                engine.execute(
                    sql_consult,
                    id=district_id
                ).fetchall()
            )
        if areas[district_id].contains(point_place):
            return district_id
    return pd.NA


def get_district_id(row):
    '''
    wrapper of in_what_district_is_this_point for use with pandas
    '''
    return in_what_district_is_this_point(row['latitude'], row['longitude'])

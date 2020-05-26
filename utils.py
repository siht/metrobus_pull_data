import pandas as pd

__all__ = (
    'construct_engine_string',
    'create_historical_points',
)

def construct_engine_string(db_config):
    '''
    recibe a dict and return a string
    '''
    return "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
        user = db_config['USER'],
        password = db_config['PASSWORD'],
        host = db_config['HOST'],
        port = db_config['PORT'],
        database = db_config['NAME'],
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
    df_new_metrobuses = df_review_metrobuses[pd.isnull(df_review_metrobuses['id'])]
    df_already_registered_metrobuses = df_review_metrobuses.dropna(subset=['id'])
    df_already_registered_metrobuses = df_already_registered_metrobuses.set_index('id')
    theres_new_metrobuses = not df_new_metrobuses.empty
    if(theres_new_metrobuses):
        last_id = (
            engine.execute(
                'SELECT id FROM metrobus_history_metrobus'
                ' ORDER BY id DESC LIMIT 1;'
            ).fetchone()
        )
        last_id = last_id[0] if last_id else -1
        current_id = last_id + 1
        next_new_id = current_id + len(df_new_metrobuses)
        new_ids = pd.Series(range(current_id, next_new_id))
        df_new_metrobuses['id'] = new_ids
        df_new_metrobuses = df_new_metrobuses.set_index('id')
        df_new_metrobuses.to_sql('metrobus_history_metrobus', engine, if_exists='append')
        return pd.concat((df_already_registered_metrobuses, df_new_metrobuses,))
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
        last_id = last_id[0] if last_id else -1
        current_id = last_id + 1
        next_new_id = current_id + len(df_new_places)
        new_ids = pd.Series(range(current_id, next_new_id))
        df_new_places['id'] = new_ids
        df_new_places = df_new_places.set_index('id')
        df_new_places.to_sql('metrobus_history_place', engine, if_exists='append')
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
    df_origin_data = df_origin_data.merge(df_places, on=['latitude', 'longitude'])
    df_origin_data.drop(['serie', 'latitude', 'longitude'], axis=1, inplace=True)
    last_id_historical_point = (
        engine.execute(
            'SELECT id FROM metrobus_history_historicalpoint'
            ' ORDER BY id DESC LIMIT 1;'
        ).fetchone()
    )
    last_id_historical_point = last_id_historical_point[0] if last_id_historical_point else -1
    current_id_historical_point = last_id_historical_point + 1
    next_id_historical_point = current_id_historical_point + df_origin_data.count()
    new_ids = pd.Series(range(current_id_historical_point, next_id_historical_point))
    df_origin_data['id'] = new_ids
    df_origin_data = df_origin_data.set_index('id')
    df_origin_data.to_sql('metrobus_history_historicalpoint', engine, if_exists='append')

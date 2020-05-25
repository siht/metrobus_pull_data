import pandas as pd

__all__ = (
    'construct_engine_string',
    'create_metrobuses_if_doesnt_exist',
    'create_places_if_doesnt_exist',
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
    '''
    df_saved_metrobuses = (
        pd.read_sql_table('metrobus_history_metrobus', engine)
    )
    df_fetched_metrobuses = (
        pd.Series(map(lambda elem: elem['serie'], data)).to_frame('serie')
    )
    df_new_metrobuses = (
        df_saved_metrobuses
        .merge(df_fetched_metrobuses, on='serie', how='right')
    )
    theres_new_metrobuses = not df_new_metrobuses.empty
    if(theres_new_metrobuses):
        last_id = (
            not df_saved_metrobuses.empty
            and df_saved_metrobuses.groupby('id').tail(1)
            or -1
        )
        current_id = last_id + 1
        next_new_id = current_id + len(df_new_metrobuses)
        new_ids = pd.Series(range(current_id, next_new_id))
        df_new_metrobuses['id'] = new_ids
        df_new_metrobuses = df_new_metrobuses.set_index('id')
        df_new_metrobuses.to_sql('metrobus_history_metrobus', engine, if_exists='append')

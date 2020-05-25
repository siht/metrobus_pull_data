__all__ = (
    'construct_engine_string',
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

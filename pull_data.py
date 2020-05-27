from urllib.request import urlopen

from config import METROBUSES_API_URL
from utils import (
    get_engine,
    filter_json_raw_data,
    create_historical_points,
)


def get_new_data():
    engine = get_engine()
    metrobuses_raw_data = urlopen(METROBUSES_API_URL).read()
    metrobuses_required_data = filter_json_raw_data(metrobuses_raw_data)
    create_historical_points(engine, metrobuses_required_data)

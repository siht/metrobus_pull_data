from urllib.request import urlopen

from celery import Celery
from celery.schedules import crontab

from config import METROBUSES_API_URL
from utils import (
    get_engine,
    filter_json_raw_data,
    create_historical_points,
)


app = Celery('pull_data', broker='pyamqp://guest@0.0.0.0//')

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    every_hour_after_1_minute = crontab(hour='*', minute=1)
    sender.add_periodic_task(
        every_hour_after_1_minute,
        get_new_data.s(),
    )


@app.task
def get_new_data():
    engine = get_engine()
    metrobuses_raw_data = urlopen(METROBUSES_API_URL).read()
    metrobuses_required_data = filter_json_raw_data(metrobuses_raw_data)
    create_historical_points(engine, metrobuses_required_data)

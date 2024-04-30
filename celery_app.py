import os

from celery import Celery
import pika
from dotenv import load_dotenv
from celery.schedules import crontab


CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")

celery_app = Celery('worker',borker=CELERY_BROKER_URL,backend=CELERY_RESULT_BACKEND)
celery_app.config_from_object('src.utils.celeryconfig')

celery_app.conf.beat_schedule = {
    'price_processing': {
        'task': '',
        'schedule': crontab()
    }
}
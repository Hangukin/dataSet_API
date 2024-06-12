import os
from datetime import timedelta

from celery import Celery
import pika
from dotenv import load_dotenv
from celery.schedules import crontab



CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")

celery_app = Celery('app', broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND, broker_connection_retry_on_startup = True, include=['src.task'])
celery_app.config_from_object('src.utils.celeryconfig')

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Seoul',
    enable_utc=False
)
'''
celery_app.conf.beat_schedule = {
    'price_processing': {
        'task': 'src.task.tasks.preprocessing_price',
        'schedule': crontab(hour=23,minute=0),
        'args':()
    }
}
'''
celery_app.conf.beat_schedule = {
    'cfr_ldgs_processing': {
        'task': 'src.task.cfr_task.cfr_price',
        'schedule': crontab(hour=21,minute=0,day_of_week='monday'),
        'args':()
    }
}

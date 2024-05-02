import os

import logging
import pika
import os
from dotenv import load_dotenv
from celery.schedules import crontab

load_dotenv()

RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
CELERY_BROKER_HOST = os.getenv("CELERY_BROKER_HOST")

def establish_rabbitmq_coonection():
    credentials = pika.PlainCredentials(username=RABBITMQ_USER, password=RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(host=CELERY_BROKER_HOST, credentials=credentials)
    
    try:
        connection = pika.BlockingConnection(parameters)
        return connection
    
    except Exception as e:
        logging.error(f"Error establishing RabbitMQ Connection {e}")
        return None
    
    
    CELERY_TIMEZONE = 'Asia/Seoul'
    CELERY_ENABLE_UTC = False
    
    # CELERYBEAT_SCHEDULE = {
    #     'add-works-every-2-minutes': {
    #         'task': 'celery_app/enqueue_jobs',
    #         'schedule': crontab(minute='*/1'),
    #         'args': (),
    #     }
    # }

    # Establish RabbitMQ connection when the module is imported
    rabbitmq_connection = establish_rabbitmq_connection()
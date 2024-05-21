from celery.utils.log import get_task_logger

from celeryapp.celery_app import celery_app
from celery.schedules import crontab

logger = get_task_logger(__name__)

@celery_app.task(bind=True)
def test(self):
    print('Celery Scheduler Test')
    return 'Celery Test'

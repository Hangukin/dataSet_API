from celery.utils.log import get_task_logger

from celery_app import celery_app
from celery.schedules import crontab

logger = get_task_logger(__name__)

@celery_app.task
def test():
    logger.info('Celery Scheduler Test')
    return 'Celery Test'

from celery.schedules import crontab
from datetime import datetime, timedelta

now = datetime.now()
yesterday = now - timedelta(days=0)

print(yesterday.strftime("%Y-%m-%d"))
print(type(yesterday.strftime("%Y-%m-%d")))
#from celery.utils.log import get_task_logger
from src.celeryapp.celery_app import celery_app
from celery.schedules import crontab
import pymysql
import pandas as pd
import numpy as np

from src.prisma import prisma
from hotel import load_room_data, load_hotel_data

@celery_app.task(bind=True)
def preprocessing_price(self):
    hotel_data = load_hotel_data()
    room_data = load_room_data()
    print('Celery Scheduler Test')

## AWS 가격 데이터 불러오기 
def aws_price_select(booked_date):
    Host = 'datamenity-v2-production.cluster-cekwmwtvw0qx.ap-northeast-2.rds.amazonaws.com'
    Port = int(3306)
    DB = 'datamenity'
    ID = 'reader'
    Password = 'wF1sd&fd*d2sQ2l_2f25j31=d'

    conn = pymysql.connect(host=Host,port=Port,user=ID, password=Password, db=DB, charset='utf8')
    cur = conn.cursor()

    sql = f"SELECT room_id, booking_date, scanned_date, stay_price \
           FROM room_price \
           WHERE booking_date = {booked_date}"
           
    price = pd.read_sql(sql,conn)
    
    conn.close()
    return price

## 로컬 DB 가격데이터 불러오기 
def local_price_select(booked_date):
    Host = '211.118.245.195'
    Port = int(3306)
    DB = 'datamenity'
    ID = 'heroworks'
    Password = 'gldjfh!@34'

    conn = pymysql.connect(host=Host,port=Port,user=ID, password=Password, db=DB, charset='utf8')
    cur = conn.cursor()

    sql = f"SELECT room_id, booking_date, scanned_date, stay_price \
           FROM room_price \
           WHERE booking_date = {booked_date}"
           
    price = pd.read_sql(sql,conn)
    
    conn.close()
    
    return price

def price_process_file(price, room, hotel):
    
    price['booking_date'] = pd.to_datetime(price['booking_date'],errors='coerce')
    price['scanned_date'] = pd.to_datetime(price['scanned_date'],errors='coerce')
    
    
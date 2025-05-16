from dotenv import load_dotenv
import os
import pandas as pd
import pymysql
import requests
import json

def call_api(datanm,dataset):
    load_dotenv()
    
    API_TOKEN = os.getenv("API_TOKEN")
    
    url = 'https://api.heroworksapi.info/api/admin/pushdb'
    data = {
    'dataSetNM': datanm,
    'dataSet': dataset
    }
    json_data = json.dumps(data)

    headers = {
    'accept' : 'application/json',
    'Content-Type': 'application/json',
    'Authorization': API_TOKEN
    }
    
    response = requests.post(url, headers=headers, data=json_data)
    result = response.json()
    
    return result

def AWS_DATABASE_CONN(sql):
    
    load_dotenv()
    
    AWS_USERNAME = os.getenv("AWS_USERNAME")
    AWS_PASSWORD = os.getenv("AWS_PASSWORD")
    AWS_DATABASE = os.getenv("AWS_DATABASE")
    AWS_HOST = os.getenv("AWS_HOST")
    AWS_SOCKET = os.getenv("AWS_SOCKET")
    
    Host = AWS_HOST
    Port = int(AWS_SOCKET)
    DB = AWS_DATABASE
    ID = AWS_USERNAME
    Password = AWS_PASSWORD
    
    conn = pymysql.connect(host=Host,port=Port,user=ID, password=Password, db=DB, charset='utf8')
    cur = conn.cursor()
    
    df = pd.read_sql(sql,conn)
    
    conn.close()

    return df

def PRC_DATABASE_CONN(sql):
    
    load_dotenv()
    
    PRC_USERNAME = os.getenv("PRC_USERNAME")
    PRC_PASSWORD = os.getenv("PRC_PASSWORD")
    PRC_DATABASE = os.getenv("PRC_DATABASE")
    PRC_HOST = os.getenv("PRC_HOST")
    PRC_SOCKET = os.getenv("PRC_SOCKET")
    
    Host = PRC_HOST
    Port = int(PRC_SOCKET)
    DB = PRC_DATABASE
    ID = PRC_USERNAME
    Password = PRC_PASSWORD
    
    conn = pymysql.connect(host=Host,port=Port,user=ID, password=Password, db=DB, charset='utf8')
    cur = conn.cursor()
    
    df = pd.read_sql(sql,conn)
    
    conn.close()

    return df

def LOCAL_DATABASE_CONN(sql):
    
    load_dotenv()
    
    LOCAL_USERNAME = os.getenv("LOCAL_USERNAME")
    LOCAL_PASSWORD = os.getenv("LOCAL_PASSWORD")
    LOCAL_DATABASE = os.getenv("LOCAL_DATABASE")
    LOCAL_HOST = os.getenv("LOCAL_HOST")
    LOCAL_SOCKET = os.getenv("LOCAL_SOCKET")
    
    Host = LOCAL_HOST
    Port = int(LOCAL_SOCKET)
    DB = LOCAL_DATABASE
    ID = LOCAL_USERNAME
    Password = LOCAL_PASSWORD
    
    conn = pymysql.connect(host=Host,port=Port,user=ID, password=Password, db=DB, charset='utf8')
    cur = conn.cursor()
    
    df = pd.read_sql(sql,conn)
    
    conn.close()

    return df

def API_DATABASE_CONN(sql):
    
    load_dotenv()
    
    API_USERNAME = os.getenv("API_USERNAME")
    API_PASSWORD = os.getenv("API_PASSWORD")
    API_DATABASE = os.getenv("API_DATABASE")
    API_HOST = os.getenv("API_HOST")
    API_SOCKET = os.getenv("API_SOCKET")
    
    Host = API_HOST
    Port = int(API_SOCKET)
    DB = API_DATABASE
    ID = API_USERNAME
    Password = API_PASSWORD
    
    conn = pymysql.connect(host=Host,port=Port,user=ID, password=Password, db=DB, charset='utf8')
    cur = conn.cursor()
    
    df = pd.read_sql(sql,conn)
    
    conn.close()

    return df

## 로컬 DB 가격데이터 불러오기 
def local_price_select(booked_date):
    
    sql = f"SELECT room_id, booking_date, scanned_date, stay_price as price, stay_remain \
           FROM room_price \
           WHERE booking_date = '{booked_date}'"
           
    price = LOCAL_DATABASE_CONN(sql)
    
    return price

## 로컬 DB 가격데이터 불러오기 범위
def local_price_cfr_select(week_ago, yesterday):
    
    sql = f"SELECT room_id, hotel_id as LDGS_ID, booking_date, scanned_date, stay_price as price, stay_remain \
           FROM room_price \
           WHERE booking_date BETWEEN '{week_ago}' AND '{yesterday}'; "
           
    price = LOCAL_DATABASE_CONN(sql)
    
    return price

## 25년 이후 로컬 DB 가격데이터 불러오기 범위
def yesterday_price_select(yesterday):
    
    sql = f"SELECT * FROM room_price WHERE booking_date = {yesterday}"
           
    price = PRC_DATABASE_CONN(sql)
    
    return price
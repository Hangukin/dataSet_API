from dotenv import load_dotenv
import os
import pandas as pd
import pymysql

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


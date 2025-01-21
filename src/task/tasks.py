import os
import requests
#from celery.utils.log import get_task_logger
from celeryapp.celery_app import celery_app
from celery.schedules import crontab

import pandas as pd
import numpy as np
import json

from src.prisma import prisma
from src.task.hotel import load_room_data, load_hotel_data
#from src.db.dataDB import db_push_price_data
from src.task.cfr_task import preprocess_price, CountByWGS84
from src.task.taskdb import AWS_DATABASE_CONN, LOCAL_DATABASE_CONN, call_api,  local_price_select, local_price_cfr_select
from datetime import datetime, timedelta
import pytz
import asyncio
from dotenv import load_dotenv

@celery_app.task(bind=True)
def preprocessing_price(self):
    
    now = datetime.now(pytz.timezone('Asia/Seoul')) # UTC에서 서울 시간대로 변경
    yesterday = now - timedelta(days=1)
    
    hotel_data = load_hotel_data()
    room_data = load_room_data()
    
    yesterday = yesterday.strftime("%Y-%m-%d")
    # price_data = aws_price_select(yesterday) # AWS 가격 데이터 불러오기 
    
    price_data = local_price_select(yesterday) # 로컬 DB 가격데이터 불러오기
    
    print('데이터 총 길이:', len(price_data))
    
    preprocessed_price_df = price_process_file(price_data, room_data, hotel_data)
    
    print('가공된 데이터 총 길이:', len(price_data))
    hotel_data_cpy = hotel_data[['LDGS_ID','LDGS_NM','CTPRVN_NM','GUGUN_NM','EMD_NM','LDGS_ROAD_ADDR']]
    
    merged_price_df = pd.merge(preprocessed_price_df, hotel_data_cpy, how='inner', on='LDGS_ID')
    
    merged_price_df = merged_price_df[['LDGS_ID','LDGS_NM','ota_type','CTPRVN_NM','GUGUN_NM','EMD_NM','LDGS_ROAD_ADDR','scanned_date_date','booking_date','weekday',
            'min_price','max_price','avg_price']]
    
    # 컬럼명 변경
    merged_price_df = merged_price_df.rename(columns={'ota_type':'OTA_NM','umd':'EMD_NM', 'LDGS_ROAD_ADDR':'LDGS_ADDR', 'scanned_date_date':'EXTRC_DE', 
                                                      'booking_date':'LDGMNT_DE','weekday':'WKDAY_NM', 'min_price':'MIN_PRC', 
                                                      'max_price':'MAX_PRC', 'avg_price':'AVRG_PRC','median_price':'MEDIAN_PRC'})
    
    print('결합데이터 확인 \n', merged_price_df)
    
    print('컬럼 확인 : ',merged_price_df.columns)
    
    result_dict = merged_price_df.to_dict(orient='records')
    datanm = 'HW_LDGS_DAIL_MAX_AVRG_MIN_PRC_INFO'
    
    result_message = json.dumps(call_api(datanm, result_dict))
    return f'Success {yesterday} Price Data Preprocessing' + '\n' + result_message

# 지역별로 상하위 1% 버리기 
def filter_quantiles(group):
    q_low = group['price'].quantile(0.01)
    q_hi = group['price'].quantile(0.99)
    return group[(group['price'] > q_low) & (group['price'] < q_hi)]

def price_process_file(price, room, hotel):
    
    price['booking_date'] = pd.to_datetime(price['booking_date'],errors='coerce')
    price['scanned_date'] = pd.to_datetime(price['scanned_date'],errors='coerce')
    
    # 날짜 차이 구하기 
    price['date_diff'] = price['booking_date'] - price['scanned_date']
    # 리드타임 30일 이하 만들기
    price = price[price['date_diff'] < pd.Timedelta(days=31)]
    price['scanned_date_date'] = price['scanned_date'].dt.date
    # OTA정보를 위해 룸테이블 로드
    room_ota=room[['room_id', 'LDGS_ID', 'ota_type']]
    # OTA 정보 업데이트
    df = pd.merge(price, room_ota, how='inner',on=['room_id'])
    
    #국내 주요 OTA만 남기기 
    domestic=['GOODCHOICE', 'YANOLJA', 'AGODA', 'EXPEDIA', 'INTERPARK',
           'BOOKING', 'HOTELS', 'DAILY']
    
    # df 테이블 적용
    df = df[df['ota_type'].isin(domestic)]
    
    # remian 1 보다 크면 전부다 1로 바꾸기
    df.loc[df['stay_remain'] > 1, 'stay_remain'] = 1
    if (df['price'] == 0).any():
        df = df.query('price != 0')
        
    # 호텔테이블 로드
    # 지역변수 붙이기
    df = pd.merge(df, hotel[['LDGS_ID', 'REGION',]], how='inner',on='LDGS_ID')
    
    try:
        
        df = df.groupby('REGION').apply(filter_quantiles).reset_index(drop=True)
        
    except Exception as e:
        print("An error occurred during quantile calculation and filtering:", str(e))
    # 최소 최대값 평균값 만들기
    df = df.groupby(['LDGS_ID','ota_type','scanned_date_date','booking_date'], as_index=False).agg(min_price=('price','min'),
                                                                                                    avg_price=('price','mean'),
                                                                                                    max_price=('price','max'))
    
    print('최소, 평균,중간값 최대값 만들기전 갯수:', df.shape[0])
    df=df.groupby(['LDGS_ID','ota_type','scanned_date_date','booking_date'], as_index=False).agg(min_price=('price','min'),
                                                                         avg_price=('price','mean'),
                                                                         median_price=('price','median'),
                                                                         max_price=('price','max'))
    
    print('최소, 평균, 중간값 최대값 만든이후 갯수:', df.shape[0])
    
    df['median_price'] = df['median_price'].round(0).astype(int)
    df['weekday'] = df['booking_date'].dt.day_name()
    df['booking_date'] = df['booking_date'].dt.strftime('%Y%m%d')
    df['scanned_date_date'] = pd.to_datetime(df['scanned_date_date'], format='%Y-%m-%d')  # 여기 현재 입렵되어있는 포맷이다. 다음에 주의하도록
    df['scanned_date_date'] = df['scanned_date_date'].dt.strftime('%Y%m%d')
    df['avg_price'] = df['avg_price'].round(0).astype(int)
    
    return df


@celery_app.task(bind=True)
def cfr_price(self):
    now = datetime.now(pytz.timezone('Asia/Seoul')) # UTC에서 서울 시간대로 변경
    yesterday = now - timedelta(days=1)
    week_ago = yesterday - timedelta(days=6)
    
    yesterday = yesterday.strftime("%Y-%m-%d")
    week_ago = week_ago.strftime("%Y-%m-%d")    
    
    hotel_data = load_hotel_data()
    #room_data = load_room_data()
    price_data = local_price_cfr_select(week_ago, yesterday)
    preprocessed_data = preprocess_price(price_data, hotel_data)
    
    radius_hotel = {}
    hotel_radius_df = pd.DataFrame()
    preprocessed_data['LDGS_LA'] = preprocessed_data['LDGS_LA'].astype(float)
    preprocessed_data['LDGS_LO'] = preprocessed_data['LDGS_LO'].astype(float)

    for ix in range(len(preprocessed_data)):
        c_id = preprocessed_data['LDGS_ID'][ix]
        lat = preprocessed_data['LDGS_LA'][ix]
        lng = preprocessed_data['LDGS_LO'][ix]
        dist = 2
        cbw = CountByWGS84(preprocessed_data,lat,lng,dist)
        result_radius = cbw.filter_by_radius() # 결과 데이터프레임
        center = int(result_radius.loc[result_radius['LDGS_ID'] == c_id ]['price'])
        max_result = round(abs(center*0.3+center),0)  # 범위 최고가
        min_result = round(abs(center*0.3-center),0)  # 범위 최저가 
        A = result_radius[result_radius['LDGS_ID'] == c_id].index  # 중심 호텔 드랍
        result_radius.drop(A,axis='index',inplace=True)
        # 1km 내 호텔이 아예 없는 경우 
        if len(result_radius) == 0 :
            radius_hotel['LDGS_ID'] = preprocessed_data['LDGS_ID'][ix]
            radius_hotel['LDGS_NM'] = preprocessed_data['LDGS_NM'][ix]
            radius_hotel['LDGMNT_TY_NM'] = preprocessed_data['LDGMNT_TY_NM'][ix]
            radius_hotel['LDGS_ADDR'] = preprocessed_data['LDGS_ROAD_ADDR'][ix]
            radius_hotel['CTY_NM'] = preprocessed_data['CTPRVN_NM'][ix]
            radius_hotel['GUGUN_NM'] = preprocessed_data['GUGUN_NM'][ix]
            radius_hotel['EMD_NM'] = preprocessed_data['EMD_NM'][ix]
            radius_hotel['LDGS_LA'] = lat
            radius_hotel['LDGS_LO'] = lng
            radius_hotel['AVRG_PRC'] = preprocessed_data['price'][ix]
            radius_hotel['CFR_LDGS_LIST_CN'] = '없음'
            radius_hotel['CFR_LDGS_CO'] = 0
            radius_hotel['CFR_LDGS_MIDDL_SMLT_PRC_LDGS_LIST_CN'] = '없음'
            radius_hotel['CFR_LDGS_MIDDL_SMLT_PRC_LDGS_AVRG_PRC'] = 0
            radius_hotel['CFR_LDGS_MIDDL_SMLT_PRC_LDGS_CO'] = 0
            hotel_radius = pd.DataFrame.from_dict(radius_hotel,'index').T
            hotel_radius_df = pd.concat([hotel_radius_df, hotel_radius])
            
        else :
            radius_hotel['LDGS_ID'] = preprocessed_data['LDGS_ID'][ix]
            radius_hotel['LDGS_NM'] = preprocessed_data['LDGS_NM'][ix]
            radius_hotel['LDGMNT_TY_NM'] = preprocessed_data['LDGMNT_TY_NM'][ix]
            radius_hotel['LDGS_ADDR'] = preprocessed_data['LDGS_ROAD_ADDR'][ix]
            radius_hotel['CTY_NM'] = preprocessed_data['CTPRVN_NM'][ix]
            radius_hotel['GUGUN_NM'] = preprocessed_data['GUGUN_NM'][ix]
            radius_hotel['EMD_NM'] = preprocessed_data['EMD_NM'][ix]
            radius_hotel['LDGS_LA'] = lat
            radius_hotel['LDGS_LO'] = lng
            radius_hotel['AVRG_PRC'] = preprocessed_data['price'][ix]
            radius_hotel['CFR_LDGS_LIST_CN'] = result_radius['LDGS_NM'].tolist()
            radius_hotel['CFR_LDGS_CO'] = len(result_radius)
            # 비슷한 가격대의 호텔의 유무 
            if len(result_radius.loc[(result_radius['price']<max_result)& (result_radius['price']>min_result)]) == 0: 
            
                radius_hotel['CFR_LDGS_MIDDL_SMLT_PRC_LDGS_AVRG_PRC'] = 0
                radius_hotel['CFR_LDGS_MIDDL_SMLT_PRC_LDGS_CO'] = 0
                radius_hotel['CFR_LDGS_MIDDL_SMLT_PRC_LDGS_LIST_CN'] = '없음'
                hotel_radius = pd.DataFrame.from_dict(radius_hotel,'index').T
                hotel_radius_df = pd.concat([hotel_radius_df, hotel_radius])
                
                
            else:  
                # 비슷한 가격들의 평균가격
                radius_hotel['CFR_LDGS_MIDDL_SMLT_PRC_LDGS_AVRG_PRC'] = int(round(result_radius.loc[(result_radius['price']<max_result)&
                                                                    (result_radius['price']>min_result)]['price'].mean()))
                # 비슷한 가격의 호텔의 수 
                radius_hotel['CFR_LDGS_MIDDL_SMLT_PRC_LDGS_CO'] =len(result_radius.loc[(result_radius['price']<max_result)&
                                                                (result_radius['price']>min_result)])
                # 주변 1km 내 비슷한 가격의 호텔이 있는 호텔 리스트
                radius_hotel['CFR_LDGS_MIDDL_SMLT_PRC_LDGS_LIST_CN'] = str(result_radius.loc[(result_radius['price']<max_result)&
                                                            (result_radius['price']>min_result)]['LDGS_NM'].tolist())
            
                hotel_radius = pd.DataFrame.from_dict(radius_hotel,'index').T
                hotel_radius_df = pd.concat([hotel_radius_df, hotel_radius])
    
    hotel_radius_df['DATA_BASE_DE'] = week_ago.replace('-','')
    hotel_radius_df['DATA_END_DE'] = yesterday.replace('-','')
    hotel_radius_df['DATA_BASE_DE'] = hotel_radius_df['DATA_BASE_DE'].astype(str)
    hotel_radius_df['DATA_END_DE'] = hotel_radius_df['DATA_END_DE'].astype(str)
    hotel_radius_df['LDGS_LA'] = hotel_radius_df['LDGS_LA'].astype(str)
    hotel_radius_df['LDGS_LO'] = hotel_radius_df['LDGS_LO'].astype(str)
    hotel_radius_df['CFR_LDGS_LIST_CN'] = hotel_radius_df['CFR_LDGS_LIST_CN'].astype(str)
    
    result_dict = hotel_radius_df.to_dict(orient='records')
    datanm = 'HW_CFR_SMR_LDGS_INFO'
    result_message = json.dumps(call_api(datanm, result_dict))
                
    return f'Success {yesterday} Price Data Preprocessing' + '\n' + result_message
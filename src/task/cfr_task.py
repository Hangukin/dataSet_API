import os
import requests
#from celery.utils.log import get_task_logger
from celeryapp.celery_app import celery_app
from celery.schedules import crontab

import pandas as pd
import numpy as np
import json

# 지근거리 유사 호텔 
from geopy.geocoders import Nominatim
from geopy.distance import great_circle
import folium

from src.prisma import prisma
from src.task.hotel import load_room_data, load_hotel_data, ldgs_list_select
#from src.db.dataDB import db_push_price_data
from src.task.taskdb import call_api, local_price_cfr_select
from datetime import datetime, timedelta
import pytz
import asyncio
from dotenv import load_dotenv

@celery_app.task(bind=True)
def cfr_price(self):
    now = datetime.now(pytz.timezone('Asia/Seoul')) # UTC에서 서울 시간대로 변경
    yesterday = now - timedelta(days=1)
    week_ago = yesterday - timedelta(days=7)
    
    yesterday = yesterday.strftime("%Y-%m-%d")
    week_ago = week_ago.strftime("%Y-%m-%d")    
    
    hotel_data = load_hotel_data()
    room_data = load_room_data()
    price_data = local_price_cfr_select(week_ago, yesterday)
    preprocessed_data = preprocess_price(price_data, room_data, hotel_data)
    
    radius_hotel = {}
    hotel_radius_df = pd.DataFrame()
    
    for ix in range(len(preprocessed_data)):
        c_name = preprocessed_data['LDGS_NM'][ix]
        lat = preprocessed_data['LDGS_LA'][ix]
        lng = preprocessed_data['LDGS_LO'][ix]
        dist = 2
        cbw = CountByWGS84(preprocessed_data,lat,lng,dist)
        result_radius = cbw.filter_by_radius() # 결과 데이터프레임
        center = int(result_radius.loc[result_radius['LDGS_NM'] == c_name ]['price'])
        max_result = round(abs(center*0.3+center),0)  # 범위 최고가
        min_result = round(abs(center*0.3-center),0)  # 범위 최저가 
        A = result_radius[result_radius['LDGS_NM'] ==c_name].index  # 중심 호텔 드랍
        result_radius.drop(A,axis='index',inplace=True)
        # 1km 내 호텔이 아예 없는 경우 
        if len(result_radius) == 0 :
            radius_hotel['LDGS_ID'] = preprocessed_data['LDGS_ID'][ix]
            radius_hotel['LDGS_NM'] = preprocessed_data['LDGS_NM'][ix]
            radius_hotel['LDGSMNT_TY_NM'] = preprocessed_data['LDGMNT_TY_NM'][ix]
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
            radius_hotel['LDGSMNT_TY_NM'] = preprocessed_data['LDGMNT_TY_NM'][ix]
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
    
    result_dict = hotel_radius_df.to_dict(orient='records')
    datanm = 'HW_CFR_SMR_LDGS_INFO'
    result_message = json.dumps(call_api(datanm, result_dict))
                
    return f'Success {yesterday} Price Data Preprocessing' + '\n' + result_message

# 지역별로 상하위 1% 버리기 
def filter_quantiles(group):
    q_low = group['price'].quantile(0.01)
    q_hi = group['price'].quantile(0.99)
    return group[(group['price'] > q_low) & (group['price'] < q_hi)]

def preprocess_price(price, room, hotel):
    # remian 1 보다 크면 전부다 1로 바꾸기
    price.loc[price['stay_remain'] > 1, 'stay_remain'] = 1
    if (price['price'] == 0).any():
        price = price.query('price != 0')
        
    rooms = room[['room_id', 'LDGS_ID']]
    # 호텔테이블 로드
    # 지역변수 붙이기
    df = pd.merge(price, rooms, how='inner',on=['room_id'])
    df = pd.merge(df, hotel[['LDGS_ID','LDGS_NM','LDGS_ROAD_ADDR','CTPRVN_NM','GUGUN_NM','EMD_NM','LDGS_LA','LDGS_LO','REGION',]], how='inner',on='LDGS_ID')
    try:
        df = df.groupby('REGION').apply(filter_quantiles).reset_index(drop=True)
    except Exception as e:
        print("An error occurred during quantile calculation and filtering:", str(e))
        
    total_df = df.groupby(['LDGS_ID','LDGS_NM','LDGS_ROAD_ADDR','CTPRVN_NM','GUGUN_NM','EMD_NM','LDGS_LA','LDGS_LO']).mean('price').reset_index()
    total_df = total_df[['LDGS_ID','LDGS_NM','LDGS_ROAD_ADDR','CTPRVN_NM','GUGUN_NM','EMD_NM','LDGS_LA','LDGS_LO','price']]
    total_df['price'] = total_df['price'].round()
    
    ldgs_list = ldgs_list_select()
    ldgs_ty_df = ldgs_list[['LDGS_ID','LDGMNT_TY_NM']]
    total_df = pd.merge(total_df,ldgs_ty_df,on='LDGS_ID',how='left')
    total_df.fillna('-',inplace=True)
    
    for idx in total_df[total_df['LDGS_LA']==0].index:
        ldgs_id = total_df['LDGS_ID'][idx]
        ldgs_list = ldgs_list[ldgs_list['LDGS_ID']==ldgs_id].reset_index(drop=True)
        if len(ldgs_list) == 0:
            continue
        lat = ldgs_list['LDGS_LA'][0]
        lng = ldgs_list['LDGS_LO'][0]
        total_df.loc[idx,'LDGS_LA'] = lat
        total_df.loc[idx,'LDGS_LO'] = lng
    
    total_df = total_df[(total_df['LDGS_LA']!=0)&(total_df['LDGS_LO']!=0)].reset_index(drop=True)
    
    return total_df
        

class CountByWGS84:

    def __init__(self, df, lat, lng, dist):
        """
        df: 데이터 프레임
        lat: 중심 위도
        lon: 중심 경도
        dist: 기준 거리(km)
        """
        self.df = df
        self.lat = lat
        self.lng = lng
        self.dist = dist
        
    def filter_by_rectangle(self):
        """
        사각 범위 내 데이터 필터링
        """
        lat_min = self.lat - 0.01 * self.dist
        lat_max = self.lat + 0.01 * self.dist

        lng_min = self.lng - 0.015 * self.dist
        lng_max = self.lng + 0.015 * self.dist

        self.points = [[lat_min, lng_min], [lat_max, lng_max]]

        result = self.df.loc[
            (self.df['LDGS_LA'] > lat_min) &
            (self.df['LDGS_LA'] < lat_max) &
            (self.df['LDGS_LO'] > lng_min) &
            (self.df['LDGS_LO'] < lng_max)
        ]
        result.index = range(len(result))

        return result
        
    def filter_by_radius(self):
        """
         반경 범위 내 데이터 필터링
        """
        # 사각 범위 내 데이터 필터링
        tmp = self.filter_by_rectangle()

        # 기준 좌표 포인트
        center = (self.lat, self.lng)
        
        result = pd.DataFrame()
        for index, row in tmp.iterrows():
            # 개별 좌표 포인트
            
            point = (row['LDGS_LA'], row['LDGS_LO'])
            d = great_circle(center, point).kilometers
            if d <= self.dist:
                result = pd.concat([result, tmp.iloc[index, :].to_frame().T])

        result.index = range(len(result))
        
        return result
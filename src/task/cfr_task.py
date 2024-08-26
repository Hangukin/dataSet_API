import os
import requests

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
    df = pd.merge(price, rooms,on=['room_id'])
    df = pd.merge(df, hotel, how='inner',on='LDGS_ID')
    try:
        df = df.groupby('REGION').apply(filter_quantiles).reset_index(drop=True)
    except Exception as e:
        print("An error occurred during quantile calculation and filtering:", str(e))
    
    df = df.drop('REGION',axis=1)
    
    total_df = df.groupby(['LDGS_ID','LDGS_NM','LDGMNT_TY_NM','LDGS_ROAD_ADDR','CTPRVN_NM','GUGUN_NM','EMD_NM','LDGS_LA','LDGS_LO']).mean('price').reset_index()
    total_df = total_df[['LDGS_ID','LDGS_NM','LDGMNT_TY_NM','LDGS_ROAD_ADDR','CTPRVN_NM','GUGUN_NM','EMD_NM','LDGS_LA','LDGS_LO','price']]
    total_df['price'] = total_df['price'].round()
    
    #ldgs_list = ldgs_list_select()
    #ldgs_ty_df = ldgs_list[['LDGS_ID','LDGMNT_TY_NM']]
    #total_df = pd.merge(total_df,ldgs_ty_df,on='LDGS_ID',how='left')
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
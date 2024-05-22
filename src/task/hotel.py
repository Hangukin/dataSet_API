#!/usr/bin/env python
import os
# coding: utf-8
# 필요라이브러리 로딩
import warnings
warnings.filterwarnings('ignore')
pd.options.display.float_format = '{:.0f}'.format
pd.set_option('display.max_rows', 200)

import pymysql
import pandas as pd
import pandas
import numpy as np
#import seaborn as sns  # 시각화 라이브러리, matplotlib보다 단순함
#import matplotlib.pyplot as plt
import re
#plt.rcParams['font.family'] = 'Malgun Gothic'

from src.task.taskdb import AWS_DATABASE_CONN, LOCAL_DATABASE_CONN
from dotenv import load_dotenv

def load_hotel_data():
    
    # SQL 쿼리 실행
    sql = "SELECT id as hotel_id, name as hotel_name, road_addr, addr, lat, lng, link FROM hotel"
    hotel = AWS_DATABASE_CONN(sql)
    hotel['road_addr'] = hotel['road_addr'].str.lstrip()
    hotel['addr'] = hotel['addr'].str.lstrip()
    hotel_cpy = preprocess_hotel_data(hotel)
    
    return hotel_cpy

def load_room_data():

    # SQL 쿼리 실행
    sql = "SELECT hotel.name as hotel_name, room.hotel_id, room.id as room_id, room.name as room_name, room.ota_type, room.created_at, room.updated_at FROM room INNER JOIN hotel ON room.hotel_id = hotel.id"
    
    room = AWS_DATABASE_CONN(sql)
    
    return room

def preprocess_hotel_data(hotel):
    # hotel 데이터프레임 복사
    lodging2 = hotel.copy()

    # 지역 매핑 딕셔너리
    region_mapping = {
            '서울': '서울',
            '부산': '부산',
            '경기': '경기',
            '인천': '인천',
            '제주': '제주',
            '울산': '울산',
            '경북': '경상',
            '경남': '경상',
            '대구': '대구',
            '경상': '경상',
            '전라': '전라',
            '전북': '전라',
            '전남': '전라',
            '충북': '충청',
            '충남': '충청',
            '광주': '광주',
            '세종': '세종',
            '대전': '대전',
            '충청': '충청',
            '강원': '강원',
            '해외': '해외'
        }

    # 'region' 열을 생성하기 위한 매핑 적용
    lodging2['region'] = np.select(
        [lodging2['addr'].str.contains(region) for region in region_mapping.keys()],
        [region_mapping[region] for region in region_mapping.keys()],
        default='해외'
    )
    
    # regino_sigugun 생성
    lodging2['gugun_nm'] = lodging2 ['addr'].str.split(' ').str[1]
    lodging2['emd_nm'] = lodging2 ['addr'].str.split(' ').str[2]
    # 필요한 컬럼만 선택
    lodging2 = lodging2[['hotel_id','hotel_name', 'road_addr', 'addr','gugun_nm','emd_nm', 'region', 'lat', 'lng']]

    # 추가 데이터 로드
    lodging = pd.read_csv('/dataset_api/app/DataFile/202405_호텔목록_위경도최신화.csv')
    lodging = lodging.rename(columns={'결정 등급':'hotel_grade', '업태구분명':'업태'})
    lodging = lodging[['hotel_id', 'hotel_grade', '객실수', '호텔규모', '업태']]

    # lodging2와 병합
    lodging = pd.merge(lodging2, lodging, how='left', on='hotel_id')
    
    lodging['market'] = lodging['region'].apply(lambda x: '해외' if x == '해외' else '국내')
    
    # 결측치 처리
    lodging['hotel_grade'].fillna(value='미등급', inplace=True)
    lodging.fillna(value='미분류', inplace=True)

    # 호텔 이름 매핑
    mapping = {
        'D호텔': '울산호텔',
        'C호텔': '스타즈호텔 남산',
        'E호텔': '머큐어앰배서더 울산',
        'F호텔': '롯데시티호텔 울산',
        'B호텔': '머큐어앰배서더 울산_2',
        'A호텔': '하이호텔펜션'}
    
    lodging['hotel_name'] = lodging['hotel_name'].map(mapping).fillna(lodging['hotel_name'])

    # 호텔 이름 정제
    lodging['hotel_name'] = lodging['hotel_name'].apply(lambda x: re.sub(r'[^a-zA-Z0-9가-힣]', '', str(x)))
    
    #데모 계정 제외 
    excluded_hotels = [11539, 11540, 11541, 11542, 11543, 11544, 11545, 11546, 11547]
    
    lodging = lodging[~lodging['hotel_id'].isin(excluded_hotels)]
    # 중복 계정 제외
    excluded_hotels_2 = [204,220,252,275,299,432,454,873,931,952,1015,1094,1233,1313,1480,1628]
    lodging = lodging[~lodging['hotel_id'].isin(excluded_hotels_2)]

    '''
    # 최신 lat, lng 업데이트 2024_04_22 버젼 
    lodging3 = pd.read_csv('C:/Users/user/Work/hero_master/lodging/updated_cor_hotel.csv', index_col=0)
    lodging = lodging.drop(['lat','lng'], axis=1)
    lodging = pd.merge(lodging,lodging3, how='left', on='hotel_id')
    '''
    return lodging
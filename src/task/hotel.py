#!/usr/bin/env python
import os
import requests
# coding: utf-8
# 필요라이브러리 로딩
import warnings
import pymysql
import pandas as pd

warnings.filterwarnings('ignore')
pd.options.display.float_format = '{:.0f}'.format
pd.set_option('display.max_rows', 200)

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
    preprocessed_hotel = preprocess_hotel_data(hotel)
    
    return preprocessed_hotel

def load_room_data():

    # SQL 쿼리 실행
    sql = "SELECT hotel.name as hotel_name, room.hotel_id, room.id as room_id, room.name as room_name, room.ota_type, room.created_at, room.updated_at FROM room INNER JOIN hotel ON room.hotel_id = hotel.id"
    
    room = AWS_DATABASE_CONN(sql)
    
    return room


def preprocess_hotel_data(hotel):
    # hotel 데이터프레임 복사
    hotel_copy = hotel.copy()

    # 지역 매핑 딕셔너리
    region_mapping = {
            '서울': '서울',
            '서울특별시':'서울',
            '부산': '부산',
            '부산광역시':'부산',
            '경기': '경기',
            '경기도':'경기',
            '인천': '인천',
            '인천광역시':'인천',
            '제주': '제주',
            '제주특별자치도':'제주',
            '울산': '울산',
            '울산광역시':'울산',
            '경북': '경상',
            '경상북도':'경상',
            '경상남도': '경상',
            '경남': '경상',
            '대구': '대구',
            '대구광역시':'대구',
            '경상': '경상',
            '전라': '전라',
            '전북': '전라',
            '전라북도': '전라',
            '전북특별자치도':'전라',
            '전남': '전라',
            '전라남도':'전라',
            '충북': '충청',
            '충청북도':'충북',
            '충남': '충청',
            '충청남도':'충청',
            '광주': '광주',
            '세종': '세종',
            '세종시':'세종',
            '대전': '대전',
            '대전광역시':'대전',
            '충청': '충청',
            '강원': '강원',
            '강원특별자치도':'강원',
            '해외': '해외'
        }

    # 'region' 열을 생성하기 위한 매핑 적용
    hotel_copy['region'] = np.select(
        [hotel_copy['road_addr'].str.contains(region) for region in region_mapping.keys()],
        [region_mapping[region] for region in region_mapping.keys()],
        default='해외'
    )
    # regino_sigugun 생성
    '''
    hotel_copy['gugun_nm'] = hotel_copy['addr'].str.split(' ').str[1]
    hotel_copy['emd_nm'] = hotel_copy['addr'].str.split(' ').str[2]
    '''
    hotel_copy = hotel_copy[hotel_copy['region']!='해외'].reset_index(drop=True) #해외 호텔 제거
    # 필요한 컬럼만 선택
    hotel_copy = hotel_copy[['hotel_id','hotel_name', 'addr','road_addr','region', 'lat', 'lng']]

    # 추가 데이터 로드
    hotel_rec = pd.read_csv('/app/DataFile/20240531_신규갱신_호텔테이블.csv')
    #hotel_rec = hotel_rec.rename(columns={'결정 등급':'hotel_grade', '업태구분명':'업태'})
    
    hotel_rec = hotel_rec[['hotel_id','cty','gugun','emd','결정 등급', '객실수', '호텔규모', '업태구분명']]
    # hotel_copy와 병합
    hotel_rec = pd.merge(hotel_copy, hotel_rec, how='left', on='hotel_id')
    #데모 계정 제외 
    excluded_hotels = [11539, 11540, 11541, 11542, 11543, 11544, 11545, 11546, 11547,1729]
    hotel_rec = hotel_rec[~hotel_rec['hotel_id'].isin(excluded_hotels)]
    # 중복 계정 제외
    excluded_hotels_2 = [204,220,252,275,299,432,454,873,931,952,1015,1094,1233,1313,1480,1628]
    hotel_rec = hotel_rec[~hotel_rec['hotel_id'].isin(excluded_hotels_2)]
    # A호텔 B호텔, 테스트 계정 제거
    excluded_hotels_3 = [10187,1760,220,213,226,1548,11708,10519,10520,11455]
    hotel_rec = hotel_rec[~hotel_rec['hotel_id'].isin(excluded_hotels_3)].reset_index(drop=True)
    
    # 시도, 구군, 읍면동 없는 호텔 구하기 
    for idx in hotel_rec[hotel_rec['cty'].isnull()].index:
        lat = hotel_rec['lat'][idx]
        lng = hotel_rec['lng'][idx]
        cty, gugun, emd = kakao_local_api(lat,lng)
        hotel_rec.loc[idx,'cty'] = cty
        hotel_rec.loc[idx,'gugun'] = gugun
        hotel_rec.loc[idx,'emd'] = emd
        
    hotel_rec['market'] = hotel_rec['region'].apply(lambda x: '해외' if x == '해외' else '국내')
    
    # 결측치 처리
    hotel_rec['결정 등급'].fillna(value='미등급', inplace=True)
    hotel_rec.fillna(value='미분류', inplace=True)
    '''
    # 호텔 이름 매핑
    mapping = {
        'D호텔': '울산호텔',
        'C호텔': '스타즈호텔 남산',
        'E호텔': '머큐어앰배서더 울산',
        'F호텔': '롯데시티호텔 울산',
        'B호텔': '머큐어앰배서더 울산_2',
        'A호텔': '하이호텔펜션'}
    
    hotel_rec['hotel_name'] = hotel_rec['hotel_name'].map(mapping).fillna(hotel_rec['hotel_name'])
    '''
    # 호텔 이름 정제
    hotel_rec['hotel_name'] = hotel_rec['hotel_name'].apply(lambda x: re.sub(r'[^a-zA-Z0-9가-힣]', '', str(x)))
    
    '''
    # 최신 lat, lng 업데이트 2024_04_22 버젼 
    lodging3 = pd.read_csv('C:/Users/user/Work/hero_master/lodging/updated_cor_hotel.csv', index_col=0)
    lodging = lodging.drop(['lat','lng'], axis=1)
    lodging = pd.merge(lodging,lodging3, how='left', on='hotel_id')
    '''
    return hotel_rec

def kakao_local_api(lat,lng):
    if lat == 0 or lng == 0:
        sido = '-'
        gugun = '-'
        emd = '-'
        return sido,gugun,emd
    url = 'https://dapi.kakao.com/v2/local/geo/coord2regioncode.json?'
    params = {'x':str(lng), 'y':str(lat)}
    headers = {"Authorization": "KakaoAK 31609d2bc92fe401a666a0ba00c50ebe"}
    places = requests.get(url, params=params, headers=headers).json()
    address_nm = places['documents'][0]['address_name']
    result = addr_split(address_nm)
    return result

def addr_split(addr):
    prv = ['경상남도','경상북도','전라남도','전라북도','강원특별자치도','충청북도','충청남도','경기도']
    emd_list = ['읍','면','동','로']
    gugun_list = ['구','군']
    adr_list = addr.split(' ')
    sido = ''
    gugun = ''
    emd = ''
    if adr_list[0] in prv:
        sido = adr_list[0]
        adr_list = adr_list[1:]
    
    if adr_list[0][-1] not in gugun_list:
        sido = adr_list[0]
        
    for adr in adr_list:
        if adr == '':
            continue
        if adr[-1] in emd_list:
            emd = adr
        elif adr[-1] in '가':
            emd = adr[:-2]
        elif adr[-1] in gugun_list:
            gugun = adr
    #print(sido,gugun,emd)
    return sido, gugun, emd

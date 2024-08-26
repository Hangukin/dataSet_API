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

from src.task.taskdb import AWS_DATABASE_CONN, LOCAL_DATABASE_CONN, API_DATABASE_CONN
from dotenv import load_dotenv

def load_hotel_data():
    
    # SQL 쿼리 실행
    sql = 'SELECT * FROM HW_LDGS_LIST'
    hotels = API_DATABASE_CONN(sql)
    #hotel['LDGS_ROAD_ADDR'] = hotel['LDGS_ROAD_ADDR'].str.lstrip()
    #hotel['LDGS_ADDR'] = hotel['LDGS_ADDR'].str.lstrip()
    preprocessed_hotel = hotels.iloc[:,[0,1,2,5,6,7,8,10,-2,-1]]
    
    return preprocessed_hotel

def load_room_data():

    # SQL 쿼리 실행
    sql = 'SELECT id as room_id, hotel_id as LDGS_ID FROM room;'
    
    room = LOCAL_DATABASE_CONN(sql)
    
    return room

'''
def preprocess_hotel_data(hotel):
    # hotel 데이터프레임 복사
    hotel_copy = hotel.copy()
    # 호텔 중복 및 테스트 계정 제거 -> 숙박시설 병합 -> 지역구분
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
            '세종특별자치시':'세종',
            '대전': '대전',
            '대전광역시':'대전',
            '충청': '충청',
            '강원': '강원',
            '강원특별자치도':'강원',
            '해외': '해외'
        }
    excluded_hotels = [11539, 11540, 11541, 11542, 11543, 11544, 11545, 11546, 11547,1729]
    hotel_copy = hotel_copy[~hotel_copy['LDGS_ID'].isin(excluded_hotels)]
    # 중복 계정 제외
    excluded_hotels_2 = [204,220,252,275,299,432,454,873,931,952,1015,1094,1233,1313,1480,1628,11510]
    hotel_copy = hotel_copy[~hotel_copy['LDGS_ID'].isin(excluded_hotels_2)]
    # A호텔 B호텔, 테스트 계정 제거
    excluded_hotels_3 = [10187,10387,11326,1760,220,213,226,1548,11708,10519,10520,11455,11572]
    hotel_copy = hotel_copy[~hotel_copy['LDGS_ID'].isin(excluded_hotels_3)].reset_index(drop=True)
    
    # 추가 데이터 로드
    hotel_rec = ldgs_list_select()
    #hotel_rec = hotel_rec.rename(columns={'결정 등급':'hotel_grade', '업태구분명':'업태'})
    
    hotel_rec = hotel_rec[['LDGS_ID','LDGS_ADDR', 'LDGS_ROAD_ADDR','CTPRVN_NM',
                           'GUGUN_NM','EMD_NM','RATING', 'LDGMNT_TY_NM','LDGS_LA', 'LDGS_LO']]
    # hotel_copy와 병합
    hotel_rec = pd.merge(hotel_copy, hotel_rec, how='inner', on='LDGS_ID')

    # 'region' 열을 생성하기 위한 매핑 적용
    hotel_rec['REGION'] = np.select(
        [hotel_rec['LDGS_ROAD_ADDR'].str.contains(region) for region in region_mapping.keys()],
        [region_mapping[region] for region in region_mapping.keys()],
        default='해외'
    )
    # regino_sigugun 생성
    
    hotel_copy['gugun_nm'] = hotel_copy['addr'].str.split(' ').str[1]
    hotel_copy['emd_nm'] = hotel_copy['addr'].str.split(' ').str[2]
    
    hotel_rec = hotel_rec[hotel_rec['REGION']!='해외'].reset_index(drop=True) #해외 호텔 제거
    # 필요한 컬럼만 선택
    # hotel_rec = hotel_rec[['LDGS_ID','LDGS_NM', 'LDGS_ADDR','LDGS_ROAD_ADDR','REGION', 'LDGS_LA', 'LDGS_LO']]

    if len(hotel_rec[hotel_rec['CTPRVN_NM'].isnull()]) != 0:
        # 시도, 구군, 읍면동 없는 호텔 구하기 
        for idx in hotel_rec[hotel_rec['CTPRVN_NM'].isnull()].index:
            lat = hotel_rec['LDGS_LA'][idx]
            lng = hotel_rec['LDGS_LO'][idx]
            cty, gugun, emd = kakao_local_api(lat,lng)
            hotel_rec.loc[idx,'CTPRVN_NM'] = cty
            hotel_rec.loc[idx,'GUGUN_NM'] = gugun
            hotel_rec.loc[idx,'EMD_NM'] = emd
        
    hotel_rec['MARKET'] = hotel_rec['REGION'].apply(lambda x: '해외' if x == '해외' else '국내')
    
    # 결측치 처리
    hotel_rec['RATING'].fillna(value='미등급', inplace=True)
    hotel_rec.fillna(value='미분류', inplace=True)
    
    # 호텔 이름 매핑
    mapping = {
        'D호텔': '울산호텔',
        'C호텔': '스타즈호텔 남산',
        'E호텔': '머큐어앰배서더 울산',
        'F호텔': '롯데시티호텔 울산',
        'B호텔': '머큐어앰배서더 울산_2',
        'A호텔': '하이호텔펜션'}
    
    hotel_rec['hotel_name'] = hotel_rec['hotel_name'].map(mapping).fillna(hotel_rec['hotel_name'])
    
    # 호텔 이름 정제
    hotel_rec['LDGS_NM'] = hotel_rec['LDGS_NM'].apply(lambda x: re.sub(r'[^a-zA-Z0-9가-힣]', '', str(x)))
    
    
    # 최신 lat, lng 업데이트 2024_04_22 버젼 
    lodging3 = pd.read_csv('C:/Users/user/Work/hero_master/lodging/updated_cor_hotel.csv', index_col=0)
    lodging = lodging.drop(['lat','lng'], axis=1)
    lodging = pd.merge(lodging,lodging3, how='left', on='hotel_id')
    
    return hotel_rec
'''

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
    address_nm = places['documents'][1]['address_name']
    result = addr_split(address_nm)
    return result

def addr_split(addr):
    met_mapping = {'서울':'서울특별시','전북특별자치도':'전라북도','전남':'전라남도','경기':'경기도','대전':'대전광역시','인천':'인천광역시',
                   '부산':'부산광역시','울산':'울산광역시','충남':'충청남도','충북':'충청북도','대구':'대구광역시','광주':'광주광역시'}
    adr_list = addr.split(' ')
    addr = addr.replace(adr_list[0],met_mapping[adr_list[0]])
    emd_list = ['읍','면','동','로']
    gugun_list = ['구','군']
    adr_list = addr.split(' ')
    sido = ''
    gugun = ''
    emd = ''
    if adr_list[0].find('세종') != -1:
        sido = adr_list[0]
        gugun = '-'
        emd = adr_list[1]
    else:
        sido = adr_list[0]
        if adr_list[1][-1] == '시':
            if adr_list[2][-1] in gugun_list:
                gugun = adr_list[1] + ' ' + adr_list[2]
                emd = adr_list[3]
            else:
                gugun = adr_list[1]
                emd = adr_list[2]
        else:
            gugun = adr_list[1]
            emd = adr_list[2]
            
    #print(sido,gugun,emd)
    return sido, gugun, emd

def ldgs_list_select():
    sql = 'SELECT LDGS_ID, LDGS_ADDR, LDGS_ROAD_ADDR, CTPRVN_NM, GUGUN_NM, EMD_NM, RATING, LDGMNT_TY_NM, LDGS_LA, LDGS_LO FROM HW_LDGS_LIST'
    
    ldgs_list = API_DATABASE_CONN(sql)

    return ldgs_list
import os
import requests

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def calculate_lead_time(df):
    """
    리드타임(booking_date - scanned_date) 계산 후 
    0일 이상 31일 이하 데이터만 필터링하여 반환합니다.
    날짜 컬럼은 str → datetime으로 변환 필요.
    """
    if df['scanned_date'].dtype == 'object':
        print('=== 수집일자 형변환===')
        df['scanned_date'] = pd.to_datetime(df['scanned_date'], format='mixed')
        
    df['scanned_date'] = pd.to_datetime(df['scanned_date']).dt.strftime('%Y-%m-%d')
    
    # 문자열인 경우 datetime으로 변환
    df['scanned_date'] = pd.to_datetime(df['scanned_date'])
    df['booking_date'] = pd.to_datetime(df['booking_date'])

    # 리드타임 계산
    df['lead_time'] = (df['booking_date'] - df['scanned_date']).dt.days

    # 리드타임이 0~31 사이인 값만 필터링
    df = df[(df['lead_time'] >= 0) & (df['lead_time'] <= 31)]
    
    return df

# 지역,업체, 북킹데이트별 상위 가격 1% 버리기
def filter_quantiles(group):
    q_hi = group['stay_price'].quantile(0.9995)
    return group[group['stay_price'] < q_hi]

def preprocess_with_hotel(df):
    """가격데이터 지역/타입/북킹데이터별로 로 이상치 처리"""
    try:
        #print('해외지역 업체 제외')
        #df=df.query('region!="해외"')
        # 허수값 삭제
        # 0제거 
        df = df[df['stay_price']>1000]
        df=df.query('stay_price<100000000')
        # 상하위 1%삭제 
        
        q_hi_by_region = df.groupby(['region'])['stay_price'].quantile(0.9995)
        #print("\n상위 0.1% 가격 by region and b_type:\n", q_hi_by_region)

        #print("지역별로 상위 0.1% 버리기 before:", df.shape)
        df = df.groupby(['region', 'rating','booking_date'], group_keys=False).apply(filter_quantiles).reset_index(drop=True)
        print("지역별로 상위 0.1% 버리기 after:", df.shape)
        print("\n")
    except Exception as e:
        print("상위 0.1% 버리기 계산중 오류발생:", str(e))
    
    return df

def region_full_lead_time(hotel_tb, df):
    lead_time = [0,7,14,21,28]
    date_df = df[['booking_date']].drop_duplicates().sort_values('booking_date').reset_index(drop=True)

    total_date = pd.DataFrame()
    for ltime in lead_time:
        for rating in df['rating'].unique():
            temp_df = date_df.copy()
            temp_df['lead_time'] = ltime
            temp_df['rating'] = rating
            total_date = pd.concat([total_date, temp_df], ignore_index=True)
    
    region = hotel_tb[['sd','sgg']].drop_duplicates().sort_values(['sd','sgg']).reset_index(drop=True)
    region_date_list = []

    for _, row in region.iterrows():
        sd = row['sd']
        sgg = row['sgg']

        # total_date 복사 후 지역정보 추가
        temp = total_date.copy()
        temp['sd'] = sd
        temp['sgg'] = sgg

        region_date_list.append(temp)

    total_region = pd.concat(region_date_list, ignore_index=True)
    
    return total_region
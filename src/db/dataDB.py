import bcrypt as bcrypt
from datetime import datetime

from src.prisma import prisma
from fastapi import HTTPException
from src.utils.config import APP_NAME, APP_SECRET_STRING

import asyncio



async def db_push_DataSet(DataSetNM,DataSet):
    if DataSetNM.isupper():
        DataSetNM = DataSetNM.lower()
    
    model = getattr(prisma,DataSetNM)
    
    created = await model.create_many( DataSet )
        
    return {'message': f'DataBase Push success from {APP_NAME}'}


async def db_select_DataSet(query):
    if query.data_nm.isupper():
        query.data_nm = query.data_nm.lower()
    else:
        query.data_nm = query.data_nm
    
    model = getattr(prisma,query.data_nm)
    
    if query.data_base_de != None:
        data = await model.find_many(where = {
            'DATA_BASE_DE' : query.data_base_de
        })
        return data
    
    if query.base_ym != None:
        data = await model.find_many(where = {
            'BASE_YM' : query.base_ym
        })
        return data
    
    if query.ldgmnt_ym != None:
        data = await model.find_many(where = {
            'LDGMNT_YM' : query.ldgmnt_ym
        })
        return data
    
    if query.ldgmnt_de != None:
        table = query.data_nm
        data = prisma.query_raw(f"SELECT * FROM {table} use index (LDGMNT_DE) WHERE {table}.LDGMNT_DE = {query.ldgmnt_de}")
        
        return data
    
    if query.stay_ym != None:
        data = await model.find_many(where={
            'CTY_NM' : query.cty_nm,
            'STAY_YM' : query.stay_ym
        })
        
        return data
    
    if query.base_year != None or query.base_mt != None or query.base_day != None:
        query_dict = {}
        if query.base_year is not None:
            query_dict['BASE_YEAR'] = query.base_year

        if query.base_mt is not None:
            query_dict['BASE_MT'] = query.base_mt

        if query.base_day is not None:
            query_dict['BASE_DAY'] = query.base_day
            
        data = await model.find_many(where = query_dict)
        
        return data


async def db_select_hotelTable(query):

    data = await prisma.hw_ldgs_list.find_many(where={'CTPRVN_NM': query.ctprvn_nm})
        
    return data

async def db_select_hotelID(query):

    data = await prisma.hotel_duplicates.find_many(where={})
        
    return data

async def db_select_hw_dail_price(query):
    table = 'HW_LDGS_DAIL_MAX_AVRG_MIN_PRC_INFO'
    if query.last_id == None: # 첫 페이지 호출 시 
        total_count = await prisma.query_raw(
            f"SELECT count(*) as total_rows FROM {table} use index(LDGMNT_DE) WHERE LDGMNT_DE = {query.ldgmnt_de}"
        )
        if int(query.ldgmnt_de) > 20240531:
            last_id = 62472773
        else:
            last_id = 1
        
        data = await prisma.query_raw(
            f"SELECT * FROM {table} use index (primary) WHERE LDGMNT_DE = {query.ldgmnt_de} AND id > {last_id} ORDER BY id ASC LIMIT 5000;"
            )
        
        result = {'total_count':total_count[0]['total_rows'],'last_id':data[-1]['id'],'result':data}
    else:
        data = await prisma.query_raw(
            f"SELECT * FROM {table} use index (primary) WHERE LDGMNT_DE = '{query.ldgmnt_de}' AND id > {query.last_id} ORDER BY id ASC LIMIT 5000;"
            )
        
        result = {'last_id':data[-1]['id'], 'result':data}
    
    return result
    
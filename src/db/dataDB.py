import bcrypt as bcrypt
from datetime import datetime

from src.prisma import prisma
from fastapi import HTTPException
from src.utils.config import APP_NAME, APP_SECRET_STRING


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
    if query.base_ym != None:
        data = await model.find_many(where = {
            'BASE_YM' : query.base_ym
        })
    if query.ldgmnt_ym != None:
        data = await model.find_many(where = {
            'LDGMNT_YM' : query.ldgmnt_ym
        })
    if query.ldgmnt_de != None:
        data = await model.find_many(where = {
            'LDGMNT_DE' : query.ldgmnt_de
        })
    if query.base_year != None or query.base_mt != None or query.base_day != None:
        data = await model.find_many(where = {
            'BASE_YEAR' : query.base_year,
            'BASE_MT' : query.base_mt,
            'BASE_DAY' : query.base_day
        })
        
    return data


def db_push_price_data(dataset):
    
    created = prisma.hw_ldgs_dail_max_avrg_min_prc_info.create_many(dataset)
    
    return f'Success MySQL Price Data Push {created} records'
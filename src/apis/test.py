import typing as t
import json
import jwt
import datetime as dt

from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from pydantic import BaseModel
from starlette import status

from src.prisma import prisma


from src.utils.config import APP_NAME, APP_SECRET_STRING
from src.apis.auth import verify_access_token
from src.db.userDB import db_push_Users, db_find_token
from src.db.dataDB import db_select_DataSet
from src.utils.auth import generate_access_token, generate_refresh_token, refresh_access_token, encrypt_password, compare_password
# generate_token, 

router = APIRouter()

@router.post("/push",
    tags=["TEST"],
    status_code=200)

async def file_upload(json_data):
    
    print(json_data)
    #tdict = decoded_data.to_dict(orient='records')
    dataset_nm = json_data['dataset_nm']
    #print(dataset_nm)
    dataset = json_data['dataset']
    #print(dataset)
    response = await db_data_insert(dataset_nm, dataset)

    return response


async def db_data_insert(dataset_nm, dataset):
    print('DB 함수 진입')
    if dataset_nm.isupper():
        dataset_nm = dataset_nm.lower() # 소문자로 변경 (prisma로 db에 넣기 위함)
    '''
    model = getattr(prisma, dataset_nm)

    # DB 저장
    #created = await model.create_many(dataset)
    await model.create_many(dataset)
    '''
    return {'message' : 'DataBase Insert success'}
import json

from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from pydantic import BaseModel

from src.apis.auth import verify_access_token
from src.prisma import prisma
from src.db.dataDB import db_push_DataSet

import pandas as pd
from src.utils.config import APP_NAME, APP_SECRET_STRING

router = APIRouter()

class test(BaseModel):
    user_id : str
    user_name : str

# 데이터셋 DB 적재 API 요청 코드
class PostDataSet(BaseModel):
    dataSetNM : str
    dataSet : list
    
@router.post(
   "/pushdb",
   tags=['admin'],
   status_code=200
)
async def PostData(
    PostDS : PostDataSet,
    payload : dict = Depends(verify_access_token)
):
    print(payload)
    # 유저 확인
    users = await prisma.users.find_unique(
        where={
            "user_id": payload['user_id']
        }
    )
    if not users:
        raise HTTPException(status_code=404, detail="User not exists")

    if not users.role == 'admin':
        raise HTTPException(status_code=403,detail='403 Forbidden')

    return await db_push_DataSet(PostDS.dataSetNM,PostDS.dataSet)

'''
@router.get(
    "/test",
    tags=["admin"],
    status_code=200,
)
async def test(
    user_info : test,
    payload : dict = Depends(verify_access_token)
):
    return {
        "payload": payload,
        'message': f'test success from {APP_NAME}'
    }
'''
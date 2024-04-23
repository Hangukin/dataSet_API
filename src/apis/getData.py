import typing as t

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

class get_data(BaseModel):
    data_nm : str
    data_base_de : t.Optional[str] = None
    base_ym : t.Optional[str] = None
    ldgmnt_ym : t.Optional[str] = None
    
@router.get(
    "/select-db",
    tags=["SELECT"],
    status_code=200,
)
async def selectDB (
    payload : dict = Depends(verify_access_token),
    query: get_data = Depends(get_data)
):
    exist = await prisma.users.find_unique(
        where={
            "user_id": payload['user_id']
        }
    )
    if not exist:
        raise HTTPException(status_code=404, detail="User not exists")
    
    if not exist.confirmed:
        print('미등록',exist.confirmed)
        raise HTTPException(status_code=401, detail="Not confirmed")
    
    data = await db_select_DataSet(query)
    
    return data
    
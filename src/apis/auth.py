import typing as t

import jwt
import datetime as dt

from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, HTTPBasic, HTTPBasicCredentials

from pydantic import BaseModel
from starlette import status
import secrets
from src.prisma import prisma

from typing import Annotated
from src.utils.config import APP_NAME, APP_SECRET_STRING

from src.db.userDB import db_push_Users, db_find_token
from src.utils.auth import generate_access_token, generate_refresh_token, refresh_access_token, encrypt_password, compare_password
# generate_token, 
router = APIRouter()
security = HTTPBasic()

class SignUp(BaseModel):
    user_id: str
    password : str
    name: str
    
class SignIn(BaseModel):
    user_id: str
    password: str
    
class RefreshToken(BaseModel):
    token : str
    
@router.post(
    "/signup",
    tags=["auth"],
    status_code=201,
)
async def signup(user: SignUp):
    '''
    url = '로컬 http://127.0.0.1:8000/api/auth/signup'
    '''
    exist = await prisma.users.find_unique(
        where={
            "user_id": user.user_id
        }
    )
    if exist:
        raise HTTPException(
            status_code=409,
            detail="User already exists",
        )
    access_token = generate_access_token(user.user_id,user.password)
    refresh_token = await generate_refresh_token(user.user_id)
    
    password = encrypt_password(user.password)
    # user.password = encrypt_password(user.password)
    
    return await db_push_Users(user.user_id,password, user.name, access_token, refresh_token)



@router.get(
    "/find-token",
    tags=["auth"],
    status_code=200,
)
async def find_token(user: SignIn):
    # Prisma 사용하여 DB 사용자 정보를 가져오기
    exist = await prisma.users.find_unique(where={"user_id": user.user_id})
    print(exist)
    if not exist:
        raise HTTPException(status_code=404, detail="User not exists")

    if not compare_password(user.password, exist.password):
        raise HTTPException(status_code=401, detail="Password not match")
    
    if not exist.confirmed:
        print('미등록',exist.confirmed)
        raise HTTPException(status_code=401, detail="Not confirmed")
    
    result = await db_find_token(exist)
    
    return result

'''
@router.get(
    "/test",
    tags=["auth"],
    status_code=200,
)
async def test(Refresh_token: RefreshToken):
    exist = await prisma.users.find_unique(
        where={
            'access_token':Refresh_token.token
        }
    )
    return {
        'users' : exist,
        "payload": 'test',
        'message': f'test success from {APP_NAME}'
    }
''' 

@router.post(
    "/refresh",
    tags=["auth"],
    status_code=200,
)
# 리프레쉬 토큰 활용하여 새 액세스 토큰 생성
async def refresh(Refresh_token: RefreshToken):
    exist = await prisma.users.find_unique(where={ 'refresh_token': Refresh_token.token} )
    
    if not exist:
        raise HTTPException(
            status_code=401,
            detail="Invalid refresh token"
        )
    
    result = await refresh_access_token(exist, Refresh_token.token)

    return result


get_bearer_token = HTTPBearer(auto_error=False)

# 접근 토큰 인증 함수
async def verify_access_token(
        authorization: t.Optional[HTTPAuthorizationCredentials] =  Depends(get_bearer_token)
):
    if authorization is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )

    token = authorization.credentials
    try:
        claims = jwt.decode(token, APP_SECRET_STRING, algorithms=["HS256"])
        print(claims)
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=401,
            detail="Signature has expired"
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=401,
            detail="Invalid token"
        )

    exist = await prisma.users.find_unique(
        where={
            'user_id': claims['user_id']
        }
    )
    if not exist:
        raise HTTPException(status_code=401, detail="유효하지 않은 토큰입니다.")
    return claims


async def get_admin(credentials: Annotated[HTTPBasicCredentials, Depends(security)]):
    exist = await prisma.users.find_unique(where={"user_id":credentials.username})
    if not exist:
        raise HTTPException(status_code=401, detail="유효하지 않은 토큰입니다.")
    
    correct_username = secrets.compare_digest(credentials.username,exist.user_id)
    correct_password = compare_password(credentials.password,exist.password)
    correct_admin = secrets.compare_digest(exist.role,'admin')
    
    if not (correct_username and correct_password and correct_admin):
        raise HTTPException(
            status_code= status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate":"Basic"},
        )
    return ""
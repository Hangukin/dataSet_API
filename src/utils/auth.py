# todo: setup jwt, password hashing, etc.
# 사용자 패스워드 해싱 
import datetime
import hashlib
import hmac
import base64

import bcrypt as bcrypt
import jwt
from fastapi import HTTPException

from src.prisma import prisma
from src.utils.config import APP_SECRET_STRING
from src.db.userDB import db_update_Users
'''
def generate_token(user_id : str, name: str):
    msg = user_id + name
    secret_str = APP_SECRET_STRING
    token = hmac.new(key=secret_str.encode('utf-8'),
                     msg=msg.encode('utf-8'),
                     digestmod=hashlib.sha256)
    
    return token.hexdigest()
'''
# PW 해싱 암호화 형태 반환 
def encrypt_password(password: str) -> str:
    print(type(password))
    return bcrypt.hashpw(
        password.encode("utf-8"), 
        bcrypt.gensalt()
).decode("utf-8")
    
# 입력된 PW, 해시된 PW 비교  
def compare_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(
        password.encode("utf-8"), # 유저 입력 비밀번호
        hashed.encode("utf-8") # DB 등록된 해쉬 비밀번호 
    )

    
# 사용자 ID와 역할 포함한 클레임 생성 - >  액세스 토큰 생성 
# 사용자 로그인 상태유지 -> 엔드포인트 접근시 사용 
def generate_access_token(user_id: int, password: str) -> str:
    claims = {
        'user_id': user_id,
        'password': password,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(days=30),
    }

    encoded_jwt = jwt.encode(claims, APP_SECRET_STRING, algorithm="HS256")
    return encoded_jwt

async def generate_refresh_token(user_id: int) -> str:
    claims = {
        'user_id': user_id,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(days=60),
    }

    encoded_jwt = jwt.encode(claims, APP_SECRET_STRING, algorithm="HS256") 
    return encoded_jwt


async def refresh_access_token(exist, token):
    
    claims = jwt.decode(token, APP_SECRET_STRING, algorithms=["HS256"])
    claims['exp'] = datetime.datetime.utcnow() + datetime.timedelta(days=30)
    result = await db_update_Users(claims,exist)
    return result
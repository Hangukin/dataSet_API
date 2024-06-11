import jwt
import datetime
import bcrypt as bcrypt
import datetime as dt
from src.prisma import prisma
from fastapi import HTTPException
from src.utils.config import APP_NAME, APP_SECRET_STRING


async def db_push_Users(user_id, password ,name, access_token, refresh_token):

    created = await prisma.api_users.create(
        {
            'user_id':user_id,
            'password':password,
            'access_token':access_token,
            'refresh_token':refresh_token,
            'name':name,
            'role':'worker',
            'confirmed':True
        }
    )
    return {'message': f'Users Register Success From {APP_NAME}', 
            'tokens':{'access_token':access_token, 'refresh_token':refresh_token}
    }

async def db_update_Users(claims, exist):
    
    new_access_token = jwt.encode(claims, APP_SECRET_STRING, algorithm="HS256")
    
    claims = {
        'user_id': exist.user_id,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(days=60),
    }

    new_refresh_token = jwt.encode(claims, APP_SECRET_STRING, algorithm="HS256") 
    
    users = await prisma.api_users.update(where={
        "user_id" : exist.user_id
    },
      data={
          'access_token' : new_access_token,
          'refresh_token' : new_refresh_token
      }                                       
)
    message = f'Create Success New Token {APP_NAME}'
    return {
        'tokens' : {'access_token': new_access_token, 'refresh_token':new_refresh_token}, 'message' : message
    }

    
# 로그인 후 접근 토큰 DB 추가
async def db_find_token(exist):
    result = await prisma.api_users.find_unique(
        where = {
            'user_id' : exist.user_id
        }
    )
    access_token = result.access_token
    refresh_token = result.refresh_token
    
    message = f'FIND SUCCESS FROM {APP_NAME}'
    return {
        "tokens": {"access_token": access_token, "refresh_token": refresh_token}, "message": message,
        "user": {
            "user_id": exist.user_id,
            "name": exist.name,
            "role": exist.role
            }
        }

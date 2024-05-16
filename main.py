import uvicorn
import datetime as datetime
import tracemalloc

#from apscheduler.schedulers.asyncio import AsyncIOScheduler
import secrets
from src.apis import apis
from src.prisma import prisma
from starlette import status

from fastapi.middleware.gzip import GZipMiddleware
from fastapi import Depends, FastAPI, HTTPException

from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi

from src.apis.auth import get_admin

app = FastAPI(
    title="BigData Platform DataSet API",
    contact={
            "name": "한국인",
            "phone": "+82-10-8930-0370",
            "email": "korea5643@heroworks.co.kr",
        },
#    docs_url=None,  redoc_url = None, openapi_url = None
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.include_router(apis, prefix="/api")


@app.on_event("startup")
async def startup():
    await prisma.connect()
   
@app.on_event("shutdown")
async def shutdown():
    await prisma.disconnect()

@app.get("/")
def read_root():
    return {"version": "1.0.0"}
'''
@app.get("/docs",include_in_schema=False)
async def get_documentation(admin: str = Depends(get_admin)):
    return get_swagger_ui_html(openapi_url="/openapi.json",title="docs")

@app.get("/openapi.json", include_in_schema=False)
async def openapi(admin: str = Depends(get_admin)):
    return get_openapi(title=app.title,  version=app.version, routes=app.routes)
'''

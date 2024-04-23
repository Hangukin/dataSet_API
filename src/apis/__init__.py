from fastapi import APIRouter

from src.apis.auth import router as auth_router
from src.apis.admin import router as admin_router
from src.apis.getData import router as get_router

apis = APIRouter()

apis.include_router(auth_router, prefix="/auth")
apis.include_router(admin_router, prefix="/admin")
apis.include_router(get_router, prefix="/getData")

__all__ = ["apis"]
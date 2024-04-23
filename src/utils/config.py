import os

from dotenv import load_dotenv

load_dotenv()
APP_NAME = os.environ.get("APP_NAME")
APP_SECRET_STRING = os.environ.get("APP_SECRET_STRING")
# app/app/config.py
import os
from fastapi.templating import Jinja2Templates

DB_CONFIG = {
    'host': os.environ['DB_HOST'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
}

templates = Jinja2Templates(directory="templates")
hidden_database = ['smt']
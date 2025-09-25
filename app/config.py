# app/app/config.py
from fastapi.templating import Jinja2Templates

DB_CONFIG = {
    'host': '192.168.1.38',
    'user': 'readonly_user',
    'password': 'kts@tsd2025'
}

templates = Jinja2Templates(directory="templates")
hidden_database = ['smt']
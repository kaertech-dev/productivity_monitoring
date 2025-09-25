#APP/app/services/production_start.py
from datetime import datetime, timedelta

def get_production_day_range():
    now = datetime.now()
    today_start = now.replace(hour=7, minute=0, second=0, microsecond=0)
    if now < today_start:
        today_start -= timedelta(days=1)
    tomorrow_start = today_start + timedelta(days=1)
    return today_start, tomorrow_start
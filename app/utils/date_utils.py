# app/utils/date_utils.py
from datetime import datetime, timedelta
from typing import Optional

def get_prod_range(
    day: Optional[str] = None,
    week: Optional[str] = None,
    month: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Compute production window (07:00 → next day 06:59:59) for given filters."""
    today = datetime.now().strftime('%Y-%m-%d')
    start = end = today

    if day:
        start = end = day

    elif week:
        try:
            year, week_num = week.split('-W')
            d = datetime.fromisocalendar(int(year), int(week_num), 1)
            start = d.strftime('%Y-%m-%d')
            end = (d + timedelta(days=6)).strftime('%Y-%m-%d')
        except Exception:
            start = end = today

    elif month:
        try:
            year, month_num = map(int, month.split('-'))
            d = datetime(year, month_num, 1)
            start = d.strftime('%Y-%m-%d')
            if month_num == 12:
                next_month = datetime(year + 1, 1, 1)
            else:
                next_month = datetime(year, month_num + 1, 1)
            end = (next_month - timedelta(days=1)).strftime('%Y-%m-%d')
        except Exception:
            start = end = today

    elif start_date and end_date:
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            start = start_dt.strftime('%Y-%m-%d')
            end = end_dt.strftime('%Y-%m-%d')
        except Exception:
            start = end = today

    elif start_date:
        start = end = start_date

    prod_start = f"{start} 07:00:00"
    prod_end = f"{(datetime.strptime(end, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')} 06:59:59"

    return start, end, prod_start, prod_end

# app/app/routes/operator_routes.py
from datetime import datetime, timedelta
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from ..services.operator_services import fetch_operator_data
from ..config import templates
from ..utils.operator_utils import sort_data, group_and_summarize
from ..utils.csv_utils import generate_csv
from typing import Optional
from fastapi.responses import StreamingResponse
import io
import csv

router = APIRouter()


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
            year, week_num = int(year), int(week_num)
            d = datetime.fromisocalendar(year, week_num, 1)  # Monday
            start = d.strftime('%Y-%m-%d')
            end = (d + timedelta(days=6)).strftime('%Y-%m-%d')
        except Exception:
            start = end = today

    elif month:
        try:
            year, month_num = map(int, month.split('-'))
            d = datetime(year, month_num, 1)
            start = d.strftime('%Y-%m-%d')
            # First day of next month
            if month_num == 12:
                next_month = datetime(year + 1, 1, 1)
            else:
                next_month = datetime(year, month_num + 1, 1)
            end = (next_month - timedelta(days=1)).strftime('%Y-%m-%d')
        except Exception:
            start = end = today

    elif start_date and end_date:
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')

            start = start.replace(hour=7, minute=0, second=0)
            end = (end + timedelta(days=1)).replace(hour=6, minute=59, second=59)
        #start, end = start_date, end_date
        except Exception:
            start = end = today

    elif start_date:
        start = end = start_date

    # Always adjust to production day boundaries
    prod_start = f"{start} 07:00:00"
    prod_end = f"{(datetime.strptime(end, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')} 06:59:59"

    return start, end, prod_start, prod_end


@router.get("/", response_class=HTMLResponse)
async def show_operator_en_today(
    request: Request,
    day: Optional[str] = None,
    week: Optional[str] = None,
    month: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    sort_by: str = "none",
    db_name: str = None
):
    start, end, prod_start, prod_end = get_prod_range(day, week, month, start_date, end_date)

    all_data, databases = fetch_operator_data(prod_start, prod_end, db_name)
    columns = ['operator_en', 'Customer', 'Model', 'Station', 'Output',
               'Target_Time', 'Cycle_Time', 'Start_Time', 'End_time', '%UTIL', 'Total_Util']

    all_data = sort_data(all_data, sort_by)
    grouped, summaries = group_and_summarize(all_data, columns)

    return templates.TemplateResponse("testing.html", {
        "request": request,
        "groups": grouped,
        "columns": columns,
        "summaries": summaries,
        "current_date": f"{start} → {end}" if start != end else start,
        "sort_by": sort_by,
        "databases": databases,
        "selected_db": db_name
    })

@router.get("/download-csv")
def download_csv(
    day: Optional[str] = None,
    week: Optional[str] = None,
    month: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db_name: Optional[str] = None
):
    start, end, prod_start, prod_end = get_prod_range(day, week, month, start_date, end_date)

    all_data, _ = fetch_operator_data(prod_start, prod_end, db_name)
    columns = ['operator_en', 'Customer', 'Model', 'Station', 'Output',
               'Target_Time', 'Start_Time', 'End_time', '%UTIL']
    #download csv button based on selected date range
    filename = f"operator_data_{start}_to_{end}.csv"

     # Create CSV in-memory
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(columns)
    for row in all_data:
        writer.writerow(row)
    buffer.seek(0)

    # Return as downloadable response
    response = StreamingResponse(buffer, media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response
    # Format CSV filename with production timestamps
    #filename_start = prod_start.replace(":", "-").replace(" ", "_")
    #filename_end = prod_end.replace(":", "-").replace(" ", "_")
    #filename = f"operator_data_{filename_start}_to_{filename_end}.csv"

    #return generate_csv(all_data, filename=filename, columns=columns)


@router.get("/api/operator_today", response_class=JSONResponse)
async def api_operator_today():
    today = datetime.now().strftime('%Y-%m-%d')
    prod_start = f"{today} 07:00:00"
    prod_end = f"{(datetime.strptime(today, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')} 06:59:59"

    all_data, databases = fetch_operator_data(prod_start, prod_end)

    return {
        "date": today,
        "count": len(all_data),
        "records": all_data
    }

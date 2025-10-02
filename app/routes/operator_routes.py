from datetime import datetime, timedelta
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from typing import Optional

from ..services.operator_services import fetch_operator_data
from ..config import templates
from ..utils.operator_utils import sort_data, group_and_summarize
from ..utils.date_utils import get_prod_range
from ..utils.rowspan_utils import add_rowspan_to_group
from ..utils.csv_utils import generate_csv_response

router = APIRouter()

def determine_filter_type(day, week, month, start_date, end_date):
    """Determine which filter type is being used"""
    if day:
        return "day"
    elif week:
        return "week"
    elif month:
        return "month"
    elif start_date and end_date:
        return "range"
    else:
        return "day"  # default

def format_date_with_month_name(date_str):
    """Convert date string to format with month name (e.g., '2025-10-02' -> 'October 02, 2025')"""
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        return date_obj.strftime('%b %d, %Y')
    except:
        return date_str

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
    filter_type = determine_filter_type(day, week, month, start_date, end_date)

    all_data, databases = fetch_operator_data(prod_start, prod_end, db_name, filter_type)
    columns = ['operator_en', 'Customer', 'Model', 'Station', 'Output',
               'Target_Time', 'Cycle_Time', 'Start_Time', 'End_time', '%UTIL', 'Total_Util']

    all_data = sort_data(all_data, sort_by)
    grouped, summaries = group_and_summarize(all_data, columns)

    for operator, records in grouped.items():
        records = add_rowspan_to_group(records, "Customer")
        records = add_rowspan_to_group(records, "Model")
        grouped[operator] = records

    # Format dates with month names
    formatted_start = format_date_with_month_name(start)
    formatted_end = format_date_with_month_name(end)
    current_date_display = f"{formatted_start} → {formatted_end}" if start != end else formatted_start

    return templates.TemplateResponse("testing.html", {
        "request": request,
        "groups": grouped,
        "columns": columns,
        "summaries": summaries,
        "current_date": current_date_display,
        "sort_by": sort_by,
        "databases": databases,
        "selected_db": db_name,
        "filter_type": filter_type
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
    filter_type = determine_filter_type(day, week, month, start_date, end_date)
    
    all_data, _ = fetch_operator_data(prod_start, prod_end, db_name, filter_type)
    columns = ['operator_en', 'Customer', 'Model', 'Station', 'Output',
               'Target_Time', 'Cycle_Time', 'Start_Time', 'End_time', '%UTIL']

    filename = f"operator_data_{start}_to_{end}.csv"
    return generate_csv_response(all_data, columns, filename)


@router.get("/api/operator_today", response_class=JSONResponse)
async def api_operator_today():
    today = datetime.now().strftime('%Y-%m-%d')
    prod_start = f"{today} 07:00:00"
    prod_end = f"{(datetime.strptime(today, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')} 06:59:59"

    all_data, databases = fetch_operator_data(prod_start, prod_end, filter_type="day")
    return {
        "date": today,
        "count": len(all_data),
        "records": all_data
    }
from ..database import get_connection
from .db_utils import get_databases, get_tables, get_columns, find_date_column
from .target_time_service import fetch_target_time
from ..config import hidden_database
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

def escape_identifier(identifier):
    """ Safely escape SQL identifiers (database/table/column names) 
    prevent SQL injection by removing backticks and validating format.
    """
    if not identifier:
        raise ValueError("Identifier cannot be empty")
    cleaned = identifier.replace('`', '')
    if not all(c.isalnum() or c in ('_', '-') for c in cleaned):
        raise ValueError(f"Invalid identifier: {identifier}")
    
    return f"`{cleaned}`"

def get_employee_name(cursor, operator_id):
    """
    Fetch employee name from attendance.list table based on operator_id (employee_num).
    Returns employee_name or the original operator_id if not found.
    """
    try:
        query = """
            SELECT employee_name
            FROM `attendance`.`list`
            WHERE employee_num = %s
            LIMIT 1
        """
        cursor.execute(query, (operator_id,))
        result = cursor.fetchone()
        
        if result and result[0]:
            return result[0]
        else:
            logger.debug(f"No employee name found for operator_id: {operator_id}")
            return operator_id  # Return original ID if name not found
    except Exception as e:
        logger.error(f"Error fetching employee name for {operator_id}: {str(e)}", exc_info=True)
        return operator_id  # Return original ID on error

def fetch_attendance_data(cursor, operator_id, prod_date):
    """
    Fetch attendance time for an operator from the central attendance database.
    Returns (time_in, time_out) or (None, None) if not found.
    
    NOTE: The attendance.raw table ONLY tracks clock-in events (type=1).
    There are no clock-out events (type=0) in the system.
    Strategy:
    - time_in = earliest clock-in timestamp of the day
    - time_out = latest clock-in timestamp + 8 hours (estimated shift length)
    """
    try:
        # Query the central attendance database for clock-in events only
        query = """
            SELECT 
                MIN(timestamp) as earliest_in,
                MAX(timestamp) as latest_in,
                COUNT(DISTINCT DATE(timestamp)) as days
            FROM `attendance`.`raw`
            WHERE employee_num = %s
            AND DATE(timestamp) = %s
            AND type = 1
        """
        cursor.execute(query, (operator_id, prod_date))
        result = cursor.fetchone()
        
        logger.debug(f"Attendance query for employee {operator_id} on {prod_date}: {result}")
        
        if result and result[0]:  # If we have at least one clock-in
            time_in = result[0]
            # Since there's no clock-out data, use the latest clock-in as reference
            # But this won't give us accurate working hours
            # Better approach: use production timestamps if available, otherwise estimate 8 hours
            logger.debug(f"Attendance found: IN={time_in}, latest_in={result[1]}")
            return time_in, None  # Return None as time_out to signal we need fallback
        
        logger.debug(f"No attendance data found for employee {operator_id} on {prod_date}")
        return None, None
    except Exception as e:
        logger.error(f"Error fetching attendance data for employee {operator_id} on {prod_date}: {str(e)}", exc_info=True)
        return None, None

def calculate_working_hours(time_in, time_out):
    """Calculate working hours between time_in and time_out."""
    if not time_in or not time_out:
        return 0
    try:
        delta = time_out - time_in
        hours = delta.total_seconds() / 3600
        return round(hours, 2)
    except Exception:
        return 0

def process_table(db, table, prod_start, prod_end, filter_type):
    """
    Process a single table for operator data.
    Uses attendance table for working hours and production data for output.
    Utilization = Output / Working Hours
    Returns list of operator data dictionaries or empty list on error.
    """
    conn = get_connection()
    cursor = conn.cursor()
    results = []
    
    try:
        # Select the database for this connection
        cursor.execute(f"USE `{db}`")
        
        columns_info = get_columns(cursor, table)
        column_names = [col[0] for col in columns_info]

        # only continue if required columns exist
        if not {"operator_en", "serial_num", "status"}.issubset(set(column_names)):
            return results

        date_column = find_date_column(columns_info)
        if not date_column:
            return results

        # Split model and station from table name
        if "_" in table:
            model, station = table.split("_", 1)
        else:
            model, station = table, ""

        # main production query - get output and times
        query = f"""
            SELECT 
                operator_en, 
                COUNT(DISTINCT serial_num) as Output, 
                MIN(`{date_column}`) as start_time,
                MAX(`{date_column}`) as end_time
            FROM `{db}`.`{table}`
            WHERE `{date_column}` BETWEEN %s AND %s
            AND `status` = 1
            GROUP BY operator_en
        """
        cursor.execute(query, (prod_start, prod_end))
        rows = cursor.fetchall()

        # Fetch target time for this model/station
        target_time = fetch_target_time(cursor, model, station)

        for row in rows:
            operator_en, output, start_time, end_time = row
            
            # Get employee name from attendance.list and use it as the operator identifier
            employee_name = get_employee_name(cursor, operator_en)
            
            # Since attendance system only records clock-ins (no clock-outs),
            # we must use production timestamps for accurate working hours
            # Calculate working hours from production data
            working_hours = calculate_working_hours(start_time, end_time)
            
            if working_hours == 0:
                # If no production data either, try attendance clock-in time
                prod_date = prod_start.split()[0]
                time_in, _ = fetch_attendance_data(cursor, operator_en, prod_date)
                if time_in:
                    # Use attendance clock-in time with production end time
                    working_hours = calculate_working_hours(time_in, end_time)
                    if working_hours == 0:
                        logger.debug(f"Could not calculate working hours for {operator_en}")
            
            # Calculate utilization based on 8-hour standard workday
            if output > 0:
                if target_time and target_time > 0:
                    # Expected output in 8 hours (28,800 seconds) at target pace
                    expected_output_8hrs = 28800 / target_time  # 8 hours = 28,800 seconds
                    # Utilization = actual output as % of expected 8-hour output
                    utilization = round((output / expected_output_8hrs) * 100, 2)
                    # Cycle time from actual working hours
                    cycle_time = round((working_hours * 3600 / output), 2) if working_hours > 0 else 0
                else:
                    # If no target time, can't calculate utilization
                    utilization = 0
                    cycle_time = 0
            else:
                utilization = 0
                cycle_time = 0

            data_dict = {
                "operator_en": employee_name,  # Now shows employee name instead of ID
                "Customer": db,
                "Model": model,
                "Station": station,
                "Output": output,
                "Target_Time": target_time or "N/A",
                "Cycle_Time": cycle_time,
                "Start_Time": start_time.strftime('%H:%M:%S') if start_time else "N/A",
                "End_time": end_time.strftime('%H:%M:%S') if end_time else "N/A",
                "%UTIL": utilization,
                "Working_Hours": working_hours
            }
            results.append(data_dict)

    except Exception as e:
        logger.error(f"Error processing table {table} in {db}: {e}")
    finally:
        cursor.close()
        conn.close()
    
    return results

def fetch_operator_data(prod_start: str, prod_end: str, db_name: str = None, filter_type: str = "day", max_workers: int = 10):
    """
    Fetch operator production data across databases within the given production
    time window. Assumes prod_start and prod_end are already properly formatted
    datetime strings ("YYYY-MM-DD HH:MM:SS").
    filter_type: "day", "week", "month", or "range" to determine cycle time calculation
    max_workers: Number of parallel threads to use (default: 10)
    """

    conn = get_connection()
    cursor = conn.cursor()

    databases = [
        db for db in get_databases(cursor)
        if db not in ("sys", "information_schema", "performance_schema", "mysql")
    ]

    if db_name and db_name not in databases:
        cursor.close()
        conn.close()
        raise ValueError(f"Database {db_name} not found")

    target_databases = [db_name] if db_name else databases

    # Collect all table tasks
    tasks = []
    for db in target_databases:
        if db.lower() in hidden_database:
            continue
        tables = get_tables(cursor, db)
        for table in tables:
            tasks.append((db, table, prod_start, prod_end, filter_type))

    cursor.close()
    conn.close()

    # Process tables in parallel
    all_data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_task = {
            executor.submit(process_table, db, table, prod_start, prod_end, filter_type): (db, table)
            for db, table, prod_start, prod_end, filter_type in tasks
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_task):
            try:
                results = future.result()
                all_data.extend(results)
            except Exception as e:
                db, table = future_to_task[future]
                logger.error(f"Error processing table {table} in {db}: {e}")
    
    # Return raw data without rowspan calculations
    # Rowspan will be calculated after grouping in the route handler
    return all_data, databases
# APP/app/services/operator_services.py
from ..database import get_connection
from .db_utils import get_databases, get_tables, get_columns, find_date_column
from .stats_utils import calculate_durations, average_of_shortest, mode_duration
from .target_time_service import fetch_target_time
from ..config import hidden_database
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

def escape_identifier(identifier):
    """ Safely escape SQL identifiers (database/table/column names) 
    prevent SQL injectoion by removing backticks and validating format.
    """
    if not identifier:
        raise ValueError("Identifier cannot be empty")
    cleaned = identifier.replace('`', '')
    if not all(c.isalnum() or c in ('_', '-') for c in cleaned):
        raise ValueError(f"Invalid identifier: {identifier}")
    
    return f"`{cleaned}`"

def process_table(db, table, prod_start, prod_end, filter_type):
    """
    Process a single table for operator data.
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

        # main production query
        query = f"""
            SELECT 
                operator_en, 
                COUNT(DISTINCT serial_num) as Output, 
                MIN(`{date_column}`) as start_time,
                MAX(`{date_column}`) as end_time,
                TIMESTAMPDIFF(HOUR, MIN(`{date_column}`), NOW()) as duration_hours
            FROM `{db}`.`{table}`
            WHERE `{date_column}` BETWEEN %s AND %s
            AND `status` = 1
            GROUP BY operator_en
        """
        cursor.execute(query, (prod_start, prod_end))
        rows = cursor.fetchall()

        for row in rows:
            operator_en, output, start_time, end_time, duration_hours = row

            #--------------------- fetch timestamps for utilization stats (keeping for potential future use)--------------------------
            cursor.execute(f"""
                SELECT `{date_column}`
                FROM `{db}`.`{table}`
                WHERE `{date_column}` BETWEEN %s AND %s
                AND operator_en = %s
                AND `status` = 1
                ORDER BY `{date_column}`
            """, (prod_start, prod_end, operator_en))
            timestamps = [r[0] for r in cursor.fetchall()]

            durations = calculate_durations(timestamps)
            avg_3_shortest = average_of_shortest(durations, n=3)
            mode_value = mode_duration(durations)

            #--------------------- split model and station--------------------------
            if "_" in table:
                model, station = table.split("_", 1)
            else:
                model, station = table, ""

            #--------------------- utilization % (old formula)--------------------------
            if filter_type == "day":
                if start_time and end_time:
                    diff_minutes = (end_time - start_time).total_seconds() / 3600.0
                    util_percent = round((diff_minutes / 12.0) * 100, 2) 
                else:
                    util_percent = 0
            elif filter_type == "week":
                if start_time and end_time:
                    diff_minutes = (end_time - start_time).total_seconds() / 3600.0
                    total_work_hours = 12.0 * 7.0 #12 hours per day * 7 days
                    util_percent = round(((diff_minutes / total_work_hours) * 100), 2)
                else:
                    util_percent = 0
            elif filter_type == "month":
                if start_time and end_time:
                    diff_minutes = (end_time - start_time).total_seconds() / 3600.0
                    days_in_period = (end_time.date() - start_time.date()).days + 1  
                    total_work_hours = 12.0 * days_in_period  
                    util_percent = round((diff_minutes / total_work_hours) * 100, 2)
                else:
                    util_percent = 0
            else:  # range
                if start_time and end_time:
                    diff_hours = (end_time - start_time).total_seconds() / 3600.0  
                    total_days = (end_time.date() - start_time.date()).days + 1  
                    total_work_hours = 12.0 * total_days  
                    util_percent = round((diff_hours / total_work_hours) * 100, 2)
                else:
                    util_percent = 0

            #--------------------- fetch target time--------------------------
            target_time = fetch_target_time(cursor, model, station)

            #--------------------- calculate cycle time--------------------------
            if filter_type == "day":
                # For single day: use actual working time / output
                if start_time and end_time:
                    time_diff_seconds = (end_time - start_time).total_seconds()
                    cycle_time = round(time_diff_seconds / output, 2) if output > 0 else 0
                else:
                    cycle_time = 0
            else:
                # For week/month/range: use total durations / output
                actual_working_seconds = sum(durations) if durations else 0
                if actual_working_seconds > 0 and output > 0:
                    cycle_time = round(actual_working_seconds / output, 2)
                else:
                    cycle_time = 0

            #--------------------- hide SMT stations--------------------------
            if "smt" in station.upper():
                station = "HIDDEN"

            results.append({
                'Customer': db.upper(),
                'Model': model.upper(),
                'Station': station.upper(),
                'operator_en': operator_en,
                'Output': output,
                'Target_Time': target_time,
                'Cycle_Time': cycle_time,
                'Start_Time': str(start_time),
                'End_time': str(end_time),
                '%UTIL': util_percent,
                'Total_Util': True
            })

    except Exception as e:
        print(f"Error processing table {table} in {db}: {e}")
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
                print(f"Error processing table {table} in {db}: {e}")
    
    # Return raw data without rowspan calculations
    # Rowspan will be calculated after grouping in the route handler
    return all_data, databases
# APP/app/services/operator_services.py
from ..database import get_connection
from .db_utils import get_databases, get_tables, get_columns, find_date_column
from .stats_utils import calculate_durations, average_of_shortest, mode_duration
from .target_time_service import fetch_target_time
from ..config import hidden_database
from collections import defaultdict


def fetch_operator_data(prod_start: str, prod_end: str, db_name: str = None, filter_type: str = "day"):
    """
    Fetch operator production data across databases within the given production
    time window. Assumes prod_start and prod_end are already properly formatted
    datetime strings ("YYYY-MM-DD HH:MM:SS").
    filter_type: "day", "week", "month", or "range" to determine cycle time calculation
    """

    conn = get_connection()
    cursor = conn.cursor()

    databases = [
        db for db in get_databases(cursor)
        if db not in ("sys", "information_schema", "performance_schema", "mysql")
    ]

    if db_name and db_name not in databases:
        raise ValueError(f"Database {db_name} not found")

    all_data = []
    target_databases = [db_name] if db_name else databases

    for db in target_databases:
        if db.lower() in hidden_database:
            continue

        tables = get_tables(cursor, db)

        for table in tables:
            try:
                columns_info = get_columns(cursor, table)
                column_names = [col[0] for col in columns_info]

                # only continue if required columns exist
                if not {"operator_en", "serial_num", "status"}.issubset(set(column_names)):
                    continue

                date_column = find_date_column(columns_info)
                if not date_column:
                    continue

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
                            diff_minutes = (end_time - start_time).total_seconds() / 60
                            util_percent = round(diff_minutes / 10.5, 2)
                        else:
                            util_percent = 0
                    elif filter_type == "week":
                        if start_time and end_time:
                            diff_minutes = (end_time - start_time).total_seconds() / 60
                            util_percent = round((diff_minutes / (10.5*7)/7), 2)
                        else:
                            util_percent = 0
                    elif filter_type == "month":
                        if start_time and end_time:
                            diff_minutes = (end_time - start_time).total_seconds() / 60
                            util_percent = round((diff_minutes / (10.5*(end_time.date() - start_time.date()).days + 1)/30), 2)
                        else:
                            util_percent = 0
                    else:  # range
                        if start_time and end_time:
                            diff_minutes = (end_time - start_time).total_seconds() / 60
                            total_days = (end_time.date() - start_time.date()).days + 1
                            util_percent = round(diff_minutes / (10.5 * total_days), 2)
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

                    all_data.append({
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
                print(f" Error processing table {table} in {db}: {e}")
                continue

    cursor.close()
    conn.close()
    
    # Return raw data without rowspan calculations
    # Rowspan will be calculated after grouping in the route handler
    return all_data, databases
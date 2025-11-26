# APP/app/services/operator_services.py
from ..database import get_connection
from .db_utils import get_databases, get_tables, get_columns, find_date_column
from .stats_utils import calculate_durations, average_of_shortest, mode_duration
from .target_time_service import fetch_target_time
from ..config import hidden_database
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

# UTC offset constant: 7 hours and 15 minutes (matching activity monitoring)
UTC_OFFSET_HOURS = 0  # Adjust this to match your timezone if needed

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

def process_table(db, table, prod_start, prod_end, filter_type):
    """
    Process a single table for operator data with break_logs-aware cycle time.
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

        # Batch fetch break_logs for all operators in this table
        if rows:
            operator_list = [row[0] for row in rows]
            
            # Adjust date range for break_logs UTC query
            from datetime import datetime
            start_dt = datetime.strptime(prod_start, '%Y-%m-%d %H:%M:%S')
            end_dt = datetime.strptime(prod_end, '%Y-%m-%d %H:%M:%S')
            adjusted_start_dt = start_dt - timedelta(hours=UTC_OFFSET_HOURS)
            adjusted_end_dt = end_dt - timedelta(hours=UTC_OFFSET_HOURS)
            
            placeholders = ','.join(['%s'] * len(operator_list))
            batch_break_logs_query = f"""
                SELECT operator_en, timestamp, action_type
                FROM projectsdb.break_logs
                WHERE operator_en IN ({placeholders})
                AND timestamp BETWEEN %s AND %s
                ORDER BY operator_en, timestamp ASC
            """
            cursor.execute(batch_break_logs_query, tuple(operator_list) + (adjusted_start_dt, adjusted_end_dt))
            all_logs = cursor.fetchall()
            
            # Group logs by operator
            logs_by_operator = {}
            for operator_en, timestamp, action_type in all_logs:
                if operator_en not in logs_by_operator:
                    logs_by_operator[operator_en] = []
                logs_by_operator[operator_en].append((timestamp, action_type))

        for row in rows:
            operator_en, output, start_time, end_time, duration_hours = row

            # Get break logs for this operator
            logs = logs_by_operator.get(operator_en, [])

            # --- Calculate cycle time using break_logs (matching activity monitoring logic) ---
            try:
                if not logs:
                    # NO BREAK LOGS - Use production timestamps as fallback
                    logger.debug(f"No break_logs for {operator_en} at {model}_{station} - using production data fallback")
                    
                    # Calculate duration from production records
                    if start_time and end_time:
                        total_duration = (end_time - start_time).total_seconds()
                        
                        # Simple cycle time: total time / output
                        if output > 0 and total_duration > 0:
                            cycle_time = round(total_duration / output, 2)
                        else:
                            cycle_time = 0
                    else:
                        cycle_time = 0
                        total_duration = 0
                    
                else:
                    # HAS BREAK LOGS - Use break_logs for accurate timing
                    # Filter break_logs to only those within the station's production timeframe
                    station_start_buffer = start_time - timedelta(minutes=30)
                    station_end_buffer = end_time + timedelta(minutes=30)
                    
                    # Convert to UTC time for comparison with break_logs
                    station_start_utc = station_start_buffer - timedelta(hours=UTC_OFFSET_HOURS)
                    station_end_utc = station_end_buffer - timedelta(hours=UTC_OFFSET_HOURS)
                    
                    # Filter logs to this station's timeframe
                    relevant_logs = [
                        (ts, action) for ts, action in logs 
                        if station_start_utc <= ts <= station_end_utc
                    ]
                    
                    if not relevant_logs:
                        # Break logs exist but none in this station's timeframe
                        logger.debug(f"No relevant break_logs for {operator_en} at {model}_{station}")
                        if start_time and end_time:
                            total_duration = (end_time - start_time).total_seconds()
                            cycle_time = round(total_duration / output, 2) if output > 0 and total_duration > 0 else 0
                        else:
                            cycle_time = 0
                            total_duration = 0
                    else:
                        # Process break logs to calculate actual working time
                        total_active_seconds = 0
                        start_time_log = None
                        work_sessions = []
                        
                        for ts, action in relevant_logs:
                            # Apply UTC offset to convert timestamps to local time
                            local_ts = ts + timedelta(hours=UTC_OFFSET_HOURS)
                            
                            if action.lower() in ["start", "play", "resume"]:
                                start_time_log = local_ts
                            elif action.lower() in ["stop", "pause", "break_start"] and start_time_log:
                                session_duration = (local_ts - start_time_log).total_seconds()
                                if session_duration > 0:
                                    total_active_seconds += session_duration
                                    work_sessions.append({
                                        'start': start_time_log,
                                        'stop': local_ts,
                                        'duration': session_duration
                                    })
                                start_time_log = None

                        # Handle case where operator started but hasn't stopped yet
                        if start_time_log:
                            current_stop = end_time
                            session_duration = (current_stop - start_time_log).total_seconds()
                            if session_duration > 0:
                                total_active_seconds += session_duration
                                work_sessions.append({
                                    'start': start_time_log,
                                    'stop': current_stop,
                                    'duration': session_duration
                                })

                        # Compute cycle time from active working time
                        if output > 0 and total_active_seconds > 0:
                            cycle_time = round(total_active_seconds / output, 2)
                            logger.debug(f"Operator {operator_en} at {model}_{station}: {len(work_sessions)} sessions, "
                                      f"{total_active_seconds:.0f}s active, {output} output, "
                                      f"cycle time: {cycle_time:.2f}s")
                        else:
                            cycle_time = 0
                        
                        total_duration = total_active_seconds

            except Exception as err:
                logger.error(f"Could not compute cycle time for {operator_en} at {model}_{station}: {err}")
                # Fallback to simple calculation
                if start_time and end_time:
                    total_duration = (end_time - start_time).total_seconds()
                    cycle_time = round(total_duration / output, 2) if output > 0 else 0
                else:
                    cycle_time = 0
                    total_duration = 0

            # --- Utilization calculation (unchanged) ---
            if filter_type == "day":
                if start_time and end_time:
                    diff_hours = (end_time - start_time).total_seconds() / 3600.0
                    util_percent = round((diff_hours / 12.0) * 100, 2) 
                else:
                    util_percent = 0
            elif filter_type == "week":
                if start_time and end_time:
                    diff_hours = (end_time - start_time).total_seconds() / 3600.0
                    total_work_hours = 12.0 * 7.0
                    util_percent = round((diff_hours / total_work_hours) * 100, 2)
                else:
                    util_percent = 0
            elif filter_type == "month":
                if start_time and end_time:
                    diff_hours = (end_time - start_time).total_seconds() / 3600.0
                    days_in_period = (end_time.date() - start_time.date()).days + 1  
                    total_work_hours = 12.0 * days_in_period  
                    util_percent = round((diff_hours / total_work_hours) * 100, 2)
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

            # Fetch target time
            target_time = fetch_target_time(cursor, model, station)

            # Hide SMT stations
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
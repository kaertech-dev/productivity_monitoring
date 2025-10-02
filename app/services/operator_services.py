from ..database import get_connection
from .db_utils import get_databases, get_tables, get_columns, find_date_column
from .stats_utils import calculate_durations, average_of_shortest, mode_duration
from .target_time_service import fetch_target_time
from ..config import hidden_database
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed



def escape_identifier(identifier):
    """
    Safely escape SQL identifiers (database/table/column names).
    Prevents SQL injection by removing backticks and validating format.
    """
    if not identifier:
        raise ValueError("Identifier cannot be empty")
    
    # Remove any backticks and validate the identifier
    cleaned = identifier.replace('`', '')
    
    # Basic validation: alphanumeric, underscores, and hyphens only
    if not all(c.isalnum() or c in ('_', '-') for c in cleaned):
        raise ValueError(f"Invalid identifier: {identifier}")
    
    return f"`{cleaned}`"


def process_table_batch(tasks, prod_start, prod_end, filter_type):
    """
    Process multiple tables in a single connection.
    More efficient use of database connections with connection pooling.
    
    Args:
        tasks: List of (db, table) tuples to process
        prod_start: Production start datetime string
        prod_end: Production end datetime string
        filter_type: "day", "week", "month", or "range"
    
    Returns:
        List of operator data dictionaries
    """
    conn = None
    cursor = None
    results = []
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        for db, table in tasks:
            try:
                results.extend(
                    process_single_table(cursor, db, table, prod_start, prod_end, filter_type)
                )
            except Exception as e:
                print(f"Error processing table {table} in {db}: {e}")
                continue
                
    except Exception as e:
        print(f"Error in batch processing: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return results


def process_single_table(cursor, db, table, prod_start, prod_end, filter_type):
    """
    Process a single table for operator data using an existing cursor.
    
    Args:
        cursor: Active database cursor
        db: Database name
        table: Table name
        prod_start: Production start datetime string
        prod_end: Production end datetime string
        filter_type: "day", "week", "month", or "range"
    
    Returns:
        List of operator data dictionaries
    """
    results = []
    
    try:
        # Safely escape identifiers to prevent SQL injection
        db_escaped = escape_identifier(db)
        table_escaped = escape_identifier(table)
        
        # Select the database
        cursor.execute(f"USE {db_escaped}")
        
        columns_info = get_columns(cursor, table)
        column_names = [col[0] for col in columns_info]

        # Only continue if required columns exist
        if not {"operator_en", "serial_num", "status"}.issubset(set(column_names)):
            return results

        date_column = find_date_column(columns_info)
        if not date_column:
            return results
        
        date_col_escaped = escape_identifier(date_column)
        
        # OPTIMIZED: Single query to get all data including timestamps
        # This replaces the previous approach of one query per operator
        query = f"""
            SELECT 
                operator_en,
                serial_num,
                {date_col_escaped}
            FROM {db_escaped}.{table_escaped}
            WHERE {date_col_escaped} BETWEEN %s AND %s
            AND `status` = 1
            ORDER BY operator_en, {date_col_escaped}
        """
        cursor.execute(query, (prod_start, prod_end))
        all_rows = cursor.fetchall()
        
        # Group data by operator
        operator_data = defaultdict(lambda: {
            'serial_nums': set(),
            'timestamps': []
        })
        
        for operator_en, serial_num, timestamp in all_rows:
            operator_data[operator_en]['serial_nums'].add(serial_num)
            operator_data[operator_en]['timestamps'].append(timestamp)
        
        # Process each operator's data
        for operator_en, data in operator_data.items():
            timestamps = sorted(data['timestamps'])
            output = len(data['serial_nums'])
            
            if not timestamps or output == 0:
                continue
            
            start_time = timestamps[0]
            end_time = timestamps[-1]
            
            # Calculate durations and statistics
            durations = calculate_durations(timestamps)
            avg_3_shortest = average_of_shortest(durations, n=3)
            mode_value = mode_duration(durations)
            
            # Split model and station
            if "_" in table:
                model, station = table.split("_", 1)
            else:
                model, station = table, ""
            
            # Calculate utilization percentage
            util_percent = calculate_utilization(
                start_time, end_time, filter_type
            )
            
            # Fetch target time
            target_time = fetch_target_time(cursor, model, station)
            
            # Calculate cycle time
            cycle_time = calculate_cycle_time(
                start_time, end_time, durations, output, filter_type
            )
            
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
        print(f"Error processing table {table} in {db}: {e}")
    
    return results


def calculate_utilization(start_time, end_time, filter_type):
    """
    Calculate utilization percentage based on filter type.
    
    Args:
        start_time: Start datetime
        end_time: End datetime
        filter_type: "day", "week", "month", or "range"
    
    Returns:
        Float: Utilization percentage
    """
    if not start_time or not end_time:
        return 0
    
    diff_hours = (end_time - start_time).total_seconds() / 3600.0
    
    if filter_type == "day":
        total_work_hours = 12.0
    elif filter_type == "week":
        total_work_hours = 12.0 * 7.0
    elif filter_type == "month":
        days_in_period = (end_time.date() - start_time.date()).days + 1
        total_work_hours = 12.0 * days_in_period
    else:  # range
        total_days = (end_time.date() - start_time.date()).days + 1
        total_work_hours = 12.0 * total_days
    
    return round((diff_hours / total_work_hours) * 100, 2)


def calculate_cycle_time(start_time, end_time, durations, output, filter_type):
    """
    Calculate cycle time based on filter type.
    
    Args:
        start_time: Start datetime
        end_time: End datetime
        durations: List of duration values in seconds
        output: Number of units produced
        filter_type: "day", "week", "month", or "range"
    
    Returns:
        Float: Cycle time in seconds
    """
    if output == 0:
        return 0
    
    if filter_type == "day":
        # For single day: use actual working time / output
        if start_time and end_time:
            time_diff_seconds = (end_time - start_time).total_seconds()
            return round(time_diff_seconds / output, 2)
        return 0
    else:
        # For week/month/range: use total durations / output
        actual_working_seconds = sum(durations) if durations else 0
        if actual_working_seconds > 0:
            return round(actual_working_seconds / output, 2)
        return 0


def fetch_operator_data(prod_start: str, prod_end: str, db_name: str = None, 
                       filter_type: str = "day", max_workers: int = 10):
    """
    Fetch operator production data across databases within the given production
    time window. 
    
    Args:
        prod_start: Start datetime string ("YYYY-MM-DD HH:MM:SS")
        prod_end: End datetime string ("YYYY-MM-DD HH:MM:SS")
        db_name: Optional specific database to query
        filter_type: "day", "week", "month", or "range" for cycle time calculation
        max_workers: Number of parallel threads (default: 10)
    
    Returns:
        Tuple: (list of operator data dicts, list of database names)
    """
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor()

        databases = [
            db for db in get_databases(cursor)
            if db not in ("sys", "information_schema", "performance_schema", "mysql")
        ]

        if db_name and db_name not in databases:
            raise ValueError(f"Database {db_name} not found")

        target_databases = [db_name] if db_name else databases

        # Collect all tables to process
        all_tasks = []
        for db in target_databases:
            if db.lower() in hidden_database:
                continue
            tables = get_tables(cursor, db)
            for table in tables:
                all_tasks.append((db, table))

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    # Batch tasks for better connection reuse
    # Each worker gets multiple tables to process with one connection
    batch_size = max(1, len(all_tasks) // max_workers)
    batches = [
        all_tasks[i:i + batch_size] 
        for i in range(0, len(all_tasks), batch_size)
    ]

    # Process batches in parallel
    all_data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(
                process_table_batch, batch, prod_start, prod_end, filter_type
            ): batch
            for batch in batches
        }
        
        for future in as_completed(future_to_batch):
            try:
                results = future.result()
                all_data.extend(results)
            except Exception as e:
                batch = future_to_batch[future]
                print(f"Error processing batch: {e}")
    
    return all_data, databases
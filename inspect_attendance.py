"""Inspect the attendance database tables"""
import mysql.connector

try:
    config = {
        'host': '192.168.1.38',
        'user': 'readonly_user',
        'password': 'kts@tsd2025'
    }
    
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    print("=" * 70)
    print("ATTENDANCE DATABASE SCHEMA")
    print("=" * 70)
    
    cursor.execute("USE `attendance`")
    
    for table_name in ['list', 'raw']:
        print(f"\n{'-' * 70}")
        print(f"Table: {table_name}")
        print('-' * 70)
        
        # Get columns
        cursor.execute(f"DESCRIBE `{table_name}`")
        cols = cursor.fetchall()
        print("\nColumns:")
        for col in cols:
            col_info = [str(c) if c is not None else 'NULL' for c in col]
            print(f"  {col_info[0]:20} {col_info[1]:20} {col_info[2]:5} {col_info[3]:20} {col_info[4]:20} {col_info[5]}")
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        count = cursor.fetchone()[0]
        print(f"\nTotal records: {count}")
        
        # Get sample data
        cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 3")
        rows = cursor.fetchall()
        print("\nSample data (first 3 rows):")
        for i, row in enumerate(rows, 1):
            print(f"  Row {i}: {row}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

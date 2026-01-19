"""Inspect attendance.raw data structure and content"""
import mysql.connector

try:
    config = {
        'host': '192.168.1.38',
        'user': 'readonly_user',
        'password': 'kts@tsd2025'
    }
    
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    cursor.execute("USE `attendance`")
    
    # Check what values type can have
    cursor.execute("""
        SELECT DISTINCT type, COUNT(*) as count
        FROM `raw`
        GROUP BY type
    """)
    
    print("Type values in attendance.raw:")
    for row in cursor.fetchall():
        print(f"  type = {row[0]}: {row[1]} records")
    
    # Check some sample records for a specific operator
    print("\nSample records for KE0052 (last 10):")
    cursor.execute("""
        SELECT id, employee_num, timestamp, type, device_ip, created_at
        FROM `raw`
        WHERE employee_num = 'KE0052'
        ORDER BY timestamp DESC
        LIMIT 10
    """)
    
    for row in cursor.fetchall():
        type_str = "IN" if row[3] == 1 else "OUT" if row[3] == 0 else f"OTHER({row[3]})"
        print(f"  {row[0]:5} | {row[1]} | {row[2]} | {type_str:5} | {row[4]}")
    
    # Check date range of data
    print("\nDate range of attendance data:")
    cursor.execute("""
        SELECT MIN(DATE(timestamp)) as earliest, MAX(DATE(timestamp)) as latest, COUNT(*) as total
        FROM `raw`
    """)
    
    row = cursor.fetchone()
    print(f"  Earliest: {row[0]}")
    print(f"  Latest: {row[1]}")
    print(f"  Total records: {row[2]}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

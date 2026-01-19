"""Verify operator_en mappings between production and attendance databases"""
import mysql.connector
from datetime import datetime, timedelta

try:
    config = {
        'host': '192.168.1.38',
        'user': 'readonly_user',
        'password': 'kts@tsd2025'
    }
    
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    print("=" * 70)
    print("OPERATOR MAPPING VERIFICATION")
    print("=" * 70)
    
    # Get sample operators from a production database
    cursor.execute("USE `kaer`")
    cursor.execute("""
        SELECT DISTINCT operator_en 
        FROM (
            SELECT DISTINCT operator_en FROM kk1_mo 
            WHERE status = 1
            UNION
            SELECT DISTINCT operator_en FROM kb1_mo
            WHERE status = 1
        ) t
        LIMIT 5
    """)
    
    sample_operators = [row[0] for row in cursor.fetchall()]
    print(f"\nSample operators from KAER database: {sample_operators}")
    
    # Check if they exist in attendance
    print("\n" + "-" * 70)
    print("Checking attendance.list table for matching employee_num:")
    print("-" * 70)
    
    cursor.execute("USE `attendance`")
    for op in sample_operators:
        cursor.execute("""
            SELECT employee_num, employee_name 
            FROM `list`
            WHERE employee_num = %s
        """, (op,))
        result = cursor.fetchone()
        if result:
            print(f"  ✓ {op} -> {result[1]}")
        else:
            print(f"  ✗ {op} -> NOT FOUND in attendance.list")
    
    # Check attendance.raw for recent activity
    print("\n" + "-" * 70)
    print("Checking attendance.raw for recent clock in/out events:")
    print("-" * 70)
    
    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')
    
    cursor.execute(f"""
        SELECT DISTINCT employee_num, COUNT(*) as event_count
        FROM `raw`
        WHERE DATE(timestamp) = %s
        LIMIT 10
    """, (today,))
    
    results = cursor.fetchall()
    if results:
        print(f"\nEmployees with activity on {today}:")
        for emp, count in results:
            print(f"  {emp}: {count} events")
    else:
        print(f"\nNo attendance events found for {today}")
        # Try yesterday
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        cursor.execute(f"""
            SELECT DISTINCT employee_num, COUNT(*) as event_count
            FROM `raw`
            WHERE DATE(timestamp) = %s
            LIMIT 10
        """, (yesterday,))
        
        results = cursor.fetchall()
        if results:
            print(f"\nEmployees with activity on {yesterday}:")
            for emp, count in results:
                print(f"  {emp}: {count} events")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

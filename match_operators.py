"""Check if operators from production match employee_num in attendance"""
import mysql.connector

try:
    config = {
        'host': '192.168.1.38',
        'user': 'readonly_user',
        'password': 'kts@tsd2025'
    }
    
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    # Get operators from production
    cursor.execute("USE `1dbfstechnologies`")
    cursor.execute("SELECT DISTINCT operator_en FROM `circadian_depanel` LIMIT 10")
    operators = [row[0] for row in cursor.fetchall()]
    
    print(f"Sample operators from production database: {operators}\n")
    
    # Check if they exist in attendance
    cursor.execute("USE `attendance`")
    
    print("Checking in attendance.list:")
    for op in operators:
        cursor.execute("SELECT employee_name FROM `list` WHERE employee_num = %s", (op,))
        result = cursor.fetchone()
        if result:
            print(f"  ✓ {op} = {result[0]}")
        else:
            # Try case-insensitive
            cursor.execute("SELECT employee_name FROM `list` WHERE LOWER(employee_num) = LOWER(%s)", (op,))
            result = cursor.fetchone()
            if result:
                print(f"  ✓ {op} (case-insensitive) = {result[0]}")
            else:
                print(f"  ✗ {op} = NOT FOUND")
    
    # Check for recent attendance data
    print("\nRecent attendance data:")
    cursor.execute("""
        SELECT DISTINCT employee_num, COUNT(*) as events
        FROM `raw`
        WHERE DATE(timestamp) >= DATE_SUB(NOW(), INTERVAL 3 DAY)
        GROUP BY employee_num
        LIMIT 10
    """)
    
    results = cursor.fetchall()
    for emp, count in results:
        print(f"  {emp}: {count} events")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

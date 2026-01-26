"""Test the updated fetch_attendance_data function"""
import mysql.connector
from datetime import datetime, timedelta

def fetch_attendance_data_test(cursor, operator_id, prod_date):
    """Test version of fetch_attendance_data"""
    try:
        # Query the central attendance database
        # Get the earliest clock-in (type=1) and latest clock-out (type=0) for the day
        query = """
            SELECT 
                MIN(CASE WHEN type = 1 THEN timestamp END) as time_in,
                MAX(CASE WHEN type = 0 THEN timestamp END) as time_out
            FROM `attendance`.`raw`
            WHERE employee_num = %s
            AND DATE(timestamp) = %s
            AND type IN (0, 1)
        """
        cursor.execute(query, (operator_id, prod_date))
        result = cursor.fetchone()
        
        print(f"Query result for {operator_id} on {prod_date}: {result}")
        
        if result and result[0] and result[1]:
            print(f"  ✓ Attendance found: IN={result[0]}, OUT={result[1]}")
            return result[0], result[1]
        
        print(f"  ✗ No attendance data found")
        return None, None
    except Exception as e:
        print(f"  ERROR: {e}")
        return None, None

try:
    config = {
        'host': '192.168.1.38',
        'user': 'readonly_user',
        'password': 'kts@tsd2025'
    }
    
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    # Test with known operators and recent dates
    test_operators = ['KE0052', 'KE0059', 'KE0062']
    
    # Try recent dates
    for days_back in range(0, 5):
        test_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        print(f"\n{'='*70}")
        print(f"Testing date: {test_date}")
        print('='*70)
        
        for op in test_operators:
            time_in, time_out = fetch_attendance_data_test(cursor, op, test_date)
            if time_in and time_out:
                hours = (time_out - time_in).total_seconds() / 3600
                print(f"  Working hours: {hours:.2f}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

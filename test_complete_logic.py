"""Comprehensive test of the updated operator_services logic"""
import mysql.connector
from datetime import datetime, timedelta

def test_operator_logic():
    """Test the complete operator data fetching logic"""
    
    config = {
        'host': '192.168.1.38',
        'user': 'readonly_user',
        'password': 'kts@tsd2025'
    }
    
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    try:
        # Get sample operators from production
        cursor.execute("USE `1dbfstechnologies`")
        cursor.execute("""
            SELECT DISTINCT operator_en
            FROM `circadian_depanel`
            LIMIT 3
        """)
        
        operators = [row[0] for row in cursor.fetchall()]
        
        # Get today's production data
        today = datetime.now().strftime('%Y-%m-%d')
        tomorrow_start = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d 07:00:00')
        today_start = f"{today} 07:00:00"
        
        print("="*70)
        print("PRODUCTION DATA TEST")
        print("="*70)
        print(f"\nSample operators: {operators}")
        print(f"Testing for date: {today}")
        
        for op in operators:
            cursor.execute(f"""
                SELECT 
                    COUNT(DISTINCT serial_num) as Output,
                    MIN(date_time) as start_time,
                    MAX(date_time) as end_time
                FROM `circadian_depanel`
                WHERE operator_en = %s
                AND DATE(date_time) = %s
            """, (op, today))
            
            result = cursor.fetchone()
            if result:
                output, start_time, end_time = result
                if output and output > 0:
                    if start_time and end_time:
                        hours = (end_time - start_time).total_seconds() / 3600
                        utilization = round((output / hours), 2) if hours > 0 else 0
                        cycle_time = round((hours * 3600 / output), 2)
                        
                        print(f"\n✓ {op}:")
                        print(f"    Output: {output}")
                        print(f"    Time range: {start_time.strftime('%H:%M:%S')} - {end_time.strftime('%H:%M:%S')}")
                        print(f"    Working hours: {hours:.2f}")
                        print(f"    Utilization: {utilization}")
                        print(f"    Cycle time: {cycle_time}s")
                    else:
                        print(f"\n✗ {op}: No time data")
                else:
                    print(f"\n✗ {op}: No output")
            else:
                print(f"\n✗ {op}: No data")
        
        # Test attendance data
        print(f"\n{'='*70}")
        print("ATTENDANCE DATA TEST")
        print("="*70)
        
        cursor.execute("USE `attendance`")
        for op in operators:
            cursor.execute(f"""
                SELECT 
                    MIN(timestamp) as earliest_in,
                    MAX(timestamp) as latest_in,
                    type
                FROM `raw`
                WHERE employee_num = %s
                AND DATE(timestamp) = %s
                GROUP BY type
                ORDER BY type
            """, (op, today))
            
            results = cursor.fetchall()
            if results:
                print(f"\n✓ {op}:")
                for row in results:
                    type_str = "Clock IN" if row[2] == 1 else "Clock OUT" if row[2] == 0 else f"Type {row[2]}"
                    print(f"    {type_str}: {row[0]} to {row[1]}")
            else:
                print(f"\n✗ {op}: No attendance data")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    test_operator_logic()

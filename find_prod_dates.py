"""Find dates with production data"""
import mysql.connector
from datetime import datetime, timedelta

config = {
    'host': '192.168.1.38',
    'user': 'readonly_user',
    'password': 'kts@tsd2025'
}

conn = mysql.connector.connect(**config)
cursor = conn.cursor()

try:
    cursor.execute("USE `1dbfstechnologies`")
    
    # Get date range of production data
    cursor.execute("""
        SELECT MIN(DATE(date_time)) as earliest, MAX(DATE(date_time)) as latest
        FROM `circadian_depanel`
    """)
    
    result = cursor.fetchone()
    if result:
        print(f"Production data date range: {result[0]} to {result[1]}")
        
        # Get days with the most data
        cursor.execute("""
            SELECT DATE(date_time) as prod_date, COUNT(*) as record_count
            FROM `circadian_depanel`
            WHERE DATE(date_time) >= DATE_SUB(NOW(), INTERVAL 10 DAY)
            GROUP BY DATE(date_time)
            ORDER BY prod_date DESC
            LIMIT 10
        """)
        
        print("\nRecent dates with production data:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} records")

finally:
    cursor.close()
    conn.close()

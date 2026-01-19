"""Quick diagnostic to check database structure for attendance data"""
import mysql.connector
import sys

try:
    # Connection details
    config = {
        'host': '192.168.1.38',
        'user': 'readonly_user',
        'password': 'kts@tsd2025'
    }
    
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    print("=" * 70)
    print("DATABASE DIAGNOSTIC REPORT")
    print("=" * 70)
    
    # Get list of databases
    cursor.execute("SHOW DATABASES")
    databases = [row[0] for row in cursor.fetchall()]
    print(f"\nFound {len(databases)} databases")
    
    # Focus on first 5 databases (excluding system ones)
    sample_dbs = [db for db in databases if db not in ['information_schema', 'mysql', 'performance_schema', 'sys']][:5]
    
    for db in sample_dbs:
        print(f"\n{'-' * 70}")
        print(f"Database: {db}")
        print('-' * 70)
        
        cursor.execute(f"USE `{db}`")
        
        # Get all tables
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Look for attendance, time, clock related tables
        attendance_tables = [t for t in tables if any(x in t.lower() for x in ['attend', 'time', 'clock', 'punch', 'break'])]
        
        print(f"Total tables: {len(tables)}")
        if attendance_tables:
            print(f"\n✓ Potential attendance-related tables found:")
            for table in attendance_tables:
                cursor.execute(f"DESCRIBE `{table}`")
                cols = cursor.fetchall()
                col_names = [c[0] for c in cols]
                print(f"  - {table}: {', '.join(col_names)}")
                
                # Get sample data
                try:
                    cursor.execute(f"SELECT * FROM `{table}` LIMIT 1")
                    sample = cursor.fetchone()
                    if sample:
                        print(f"    Sample: {sample}")
                except:
                    pass
        else:
            print(f"\n✗ No attendance-related tables found")
            print(f"  Available tables: {', '.join(tables[:10])}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

"""List available databases and their production tables"""
import mysql.connector

try:
    config = {
        'host': '192.168.1.38',
        'user': 'readonly_user',
        'password': 'kts@tsd2025'
    }
    
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    # Get all databases
    cursor.execute("SHOW DATABASES")
    all_dbs = [row[0] for row in cursor.fetchall()]
    
    # Filter out system databases
    prod_dbs = [db for db in all_dbs if db not in ['information_schema', 'mysql', 'performance_schema', 'sys', 'attendance', 'CSV_DB']]
    
    print(f"Found {len(prod_dbs)} production databases")
    print("\nFirst 10 databases:")
    for i, db in enumerate(prod_dbs[:10], 1):
        print(f"  {i}. {db}")
    
    # Get a sample production database
    if prod_dbs:
        sample_db = prod_dbs[0]
        print(f"\n{'='*70}")
        print(f"Using sample database: {sample_db}")
        print('='*70)
        
        cursor.execute(f"USE `{sample_db}`")
        
        # List tables
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"\nTables: {', '.join(tables[:5])}...")
        
        # Get a sample operator
        if tables:
            table = tables[0]
            cursor.execute(f"SELECT DISTINCT operator_en FROM `{table}` WHERE status = 1 LIMIT 5")
            ops = [row[0] for row in cursor.fetchall()]
            print(f"\nSample operators from {table}: {ops}")
            
            # Check in attendance
            cursor.execute("USE `attendance`")
            print(f"\nChecking in attendance.list:")
            for op in ops:
                cursor.execute("SELECT employee_name FROM `list` WHERE employee_num = %s", (op,))
                result = cursor.fetchone()
                if result:
                    print(f"  ✓ {op} = {result[0]}")
                else:
                    print(f"  ✗ {op} = NOT FOUND")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

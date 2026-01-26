"""Check table structure in production database"""
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
    
    # Get a sample production database
    if prod_dbs:
        sample_db = prod_dbs[0]
        print(f"Database: {sample_db}")
        
        cursor.execute(f"USE `{sample_db}`")
        
        # List tables
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"Tables: {tables[:5]}\n")
        
        # Get a sample table and its structure
        if tables:
            table = tables[0]
            cursor.execute(f"DESCRIBE `{table}`")
            cols = cursor.fetchall()
            
            print(f"Columns in {table}:")
            for col in cols:
                print(f"  {col[0]:20} {col[1]}")
            
            # Try to get operators
            cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table}' AND TABLE_SCHEMA='{sample_db}'")
            col_names = [row[0] for row in cursor.fetchall()]
            
            if 'operator_en' in col_names:
                cursor.execute(f"SELECT DISTINCT operator_en FROM `{table}` LIMIT 5")
                ops = [row[0] for row in cursor.fetchall()]
                print(f"\nOperators: {ops}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

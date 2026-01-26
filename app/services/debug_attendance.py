#!/usr/bin/env python3
"""
Debug script to check attendance table structure and sample data.
"""
from ..database import get_connection
from ..config import hidden_database
from .db_utils import get_databases
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def check_attendance_tables():
    """Check all databases for attendance table."""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        databases = get_databases(cursor, hidden_database)
        
        for db in databases[:3]:  # Check first 3 databases
            print(f"\n{'='*60}")
            print(f"Checking database: {db}")
            print('='*60)
            
            cursor.execute(f"USE `{db}`")
            
            # List all tables
            cursor.execute("SHOW TABLES")
            tables = [t[0] for t in cursor.fetchall()]
            print(f"Tables in {db}: {', '.join(tables[:10])}")
            
            # Check for attendance table
            if 'attendance' in tables:
                print(f"\n✓ Attendance table FOUND in {db}")
                
                # Get columns
                cursor.execute("DESCRIBE attendance")
                columns = cursor.fetchall()
                print(f"\nColumns in attendance table:")
                for col in columns:
                    print(f"  - {col[0]}: {col[1]}")
                
                # Get sample data
                cursor.execute("SELECT * FROM attendance LIMIT 3")
                rows = cursor.fetchall()
                print(f"\nSample data (first 3 rows):")
                for i, row in enumerate(rows, 1):
                    print(f"  Row {i}: {row}")
                
                # Get data count by operator
                cursor.execute("SELECT operator_en, COUNT(*) FROM attendance GROUP BY operator_en LIMIT 5")
                rows = cursor.fetchall()
                print(f"\nOperator record counts (sample):")
                for op, count in rows:
                    print(f"  - {op}: {count} records")
                    
            else:
                print(f"\n✗ Attendance table NOT FOUND in {db}")
                # Suggest similar tables
                for table in tables:
                    if 'attend' in table.lower() or 'clock' in table.lower():
                        print(f"  → Did you mean: {table}?")
            
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_attendance_tables()

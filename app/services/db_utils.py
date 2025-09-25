#APP/app/services/db_utils.py
def get_databases(cursor):
    cursor.execute("SHOW DATABASES")
    return [db[0] for db in cursor.fetchall()]


def get_tables(cursor, db):
    cursor.execute(f"USE `{db}`")
    cursor.execute("SHOW TABLES")
    return [tbl[0] for tbl in cursor.fetchall()]


def get_columns(cursor, table):
    cursor.execute(f"DESCRIBE `{table}`")
    return cursor.fetchall()


def find_date_column(columns_info):
    for col in columns_info:
        col_name = col[0].lower()
        col_type = col[1].lower()
        if ('date' in col_name or 'time' in col_name) and ('date' in col_type or 'timestamp' in col_type):
            return col[0]
    return None

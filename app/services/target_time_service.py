#APP/app/services/target_time_services.py
def fetch_target_time(cursor, model, station):
    try:
        cursor.execute("""
            SELECT process_time 
            FROM production_plan.target_time
            WHERE model = %s AND station = %s
            LIMIT 1
        """, (model.upper(), station.upper()))
        res = cursor.fetchone()
        if res:
            return res[0]
    except Exception:
        return None
    return None

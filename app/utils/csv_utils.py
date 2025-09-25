# app/utils/csv_utils.py
import pandas as pd
from io import StringIO
from fastapi.responses import StreamingResponse

def generate_csv(all_data, start_date, end_date, columns):
    all_data_filtered = [
        {key: row[key] for key in columns if key in row} for row in all_data
    ]

    df = pd.DataFrame(all_data_filtered, columns=columns)
    stream = StringIO()
    df.to_csv(stream, index=False)
    stream.seek(0)
    return StreamingResponse(
        stream,
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=operator_data_{start_date}_to_{end_date}.csv"
        }
    )

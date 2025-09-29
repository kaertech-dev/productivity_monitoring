# app/utils/csv_utils.py
import io
import csv
from fastapi.responses import StreamingResponse

def generate_csv_response(data, columns, filename):
    """Generate CSV streaming response from data."""
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(columns)
    for row in data:
        writer.writerow([row.get(col, '') for col in columns])
    buffer.seek(0)

    response = StreamingResponse(io.StringIO(buffer.getvalue()), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response

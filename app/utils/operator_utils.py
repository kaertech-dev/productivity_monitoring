# app/utils/operator_utils.py
from collections import defaultdict

def sort_data(all_data, sort_by: str):
    if sort_by == "az":
        all_data.sort(key=lambda x: x["operator_en"])
    elif sort_by == "za":
        all_data.sort(key=lambda x: x["operator_en"], reverse=True)
    elif sort_by == "time":
        all_data.sort(key=lambda x: x["Start_Time"])
    return all_data

def group_and_summarize(all_data, columns):
    grouped = defaultdict(list)
    summaries = {}
    total_utils_all = []

    for d in all_data:
        # Keep the record as a dictionary instead of converting to tuple
        grouped[d['operator_en']].append(d)

    for operator, rows in grouped.items():
        # Access %UTIL from dictionary instead of tuple index
        total_Util = sum(float(row.get("%UTIL", 0)) for row in rows)
        summaries[operator] = {
            "%UTIL": round(total_Util, 2)
        }
        total_utils_all.append(total_Util)

    if total_utils_all:
        avg_util = sum(total_utils_all) / len(total_utils_all)
        summaries["__AVERAGE__"] = {
            "%UTIL": round(avg_util, 2)
        }

    return grouped, summaries

def preprocess_for_merge(records, merge_key="Station"):
    processed = []
    current = None

    for record in records:
        key_value = record[merge_key]
        if not current or key_value != current["value"]:
            # start new group
            current = {
                "value": key_value,
                "rows": [record]
            }
            processed.append(current)
        else:
            # add to current group
            current["rows"].append(record)

    return processed
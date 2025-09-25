# app/utils/operator.py
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
        grouped[d['operator_en']].append(tuple(d[col] for col in columns))

    for operator, rows in grouped.items():
        total_Util = sum(float(r[columns.index("%UTIL")]) for r in rows)
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

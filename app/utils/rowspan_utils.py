# app/utils/rowspan_utils.py
def add_rowspan_to_group(records, col_name):
    """Add rowspan counts for consecutive identical values within a group."""
    n = len(records)
    i = 0
    while i < n:
        val = records[i][col_name]
        count = 1
        j = i + 1
        while j < n and records[j][col_name] == val:
            count += 1
            j += 1
        for k in range(i, j):
            records[k][f"{col_name}_rowspan"] = count if k == i else 0
        i = j
    return records

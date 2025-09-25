#APP/app/services/stats_utils.py
import numpy as np
from scipy import stats

def calculate_durations(timestamps):
    durations = []
    for i in range(1, len(timestamps)):
        diff = (timestamps[i] - timestamps[i - 1]).total_seconds()
        if diff > 0:
            durations.append(diff)
    return sorted(durations)

def average_of_shortest(durations, n=3):
    if len(durations) >= n:
        return round(sum(durations[:n]) / n, 2)
    return 0

def mode_duration(durations):
    if not durations:
        return 0
    modes = np.round(durations, 2)
    modes_results = stats.mode(modes, keepdims=False)
    return float(modes_results.mode) if not np.isnan(modes_results.mode) else 0

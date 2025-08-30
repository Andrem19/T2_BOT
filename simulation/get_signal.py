from datetime import datetime, timezone
import shared_vars as sv
import numpy as np
import talib


def signal(data, i: int) -> dict:

    decision = False
    dt = datetime.fromtimestamp(data[i][0]/1000)
    
    dt_now = datetime.now(timezone.utc)
    
    hour = dt.hour
    weekday = dt.weekday()
    
    weekdays_set = [5,6] if dt_now.weekday() in [5, 6] else [0,1,2,3,4]
    if hour == dt_now.hour and weekday in weekdays_set:
        decision = True
    
    return {
        "decision": decision,
    }

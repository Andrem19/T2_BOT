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
    if hour == dt_now.hour and weekday in [0,1,2,3,4]:
        decision = True
    
    return {
        "decision": decision,
    }

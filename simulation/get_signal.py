from datetime import datetime
import shared_vars as sv
import numpy as np
import talib


def signal(data, i: int) -> dict:

    decision = False
    dt = datetime.fromtimestamp(data[i][0]/1000)
    
    hour = dt.hour
    minute = dt.minute
    close_6h_ago = data[i-420][4]
    close_now = data[i-1][4]
    if hour == 8 and minute == 59:

        if close_6h_ago*1.005 < close_now:
            decision=True
    
    return {
        "decision": decision,
    }

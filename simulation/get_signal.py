from datetime import datetime
import shared_vars as sv
import numpy as np
import talib


def signal(data, i: int) -> dict:

    # decision = False
    # dt = datetime.fromtimestamp(data[i][0]/1000)
    
    # hour = dt.hour
    # weekday = dt.weekday()
    # if hour in [11, 12] and weekday in [0,1,2,3,4]:
    #     decision = True
    
    return {
        "decision": True,
    }

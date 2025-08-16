import math
import traceback
import shared_vars as sv


def simulation(data, i, fut_calc, params, end_idx):
    try:
        ask_cost  = float(fut_calc['ask'])
        down_pnl  = float(fut_calc['pnl_lower'])
        up_pnl    = float(fut_calc['pnl_upper'])
        qty       = float(fut_calc['qty'])
        
        strike = 0
        mode = params['mode'].lower()
        if mode == 'put':
            strike = params['lower_perc']-fut_calc['p_t']
        else:
            strike = params['upper_perc']-fut_calc['p_t']
        
        start = data[i][0]
        finish_time = 0
        breakout = ''
        it = i
        open_price = data[i][1]
        
        
        high_border = open_price * (1+params['upper_perc'])
        lower_border = open_price * (1-params['lower_perc'])
        middle_2 = 0
        if mode == 'put':
            middle_2 = open_price * (1-strike)
        else:
            middle_2 = open_price * (1+strike)

        all_fees = -0.80
        pnl = 0
        duration = 0
        close_price = 0
        

        while True:
            high = data[it][2]
            low = data[it][3]
            finish_time = data[it][0]
                
            duration = (finish_time - start) / 60_000.0
            
            
            if it > end_idx:
                return None, it

            if duration > (params['hours']*60):
                close_price = data[it][1]
                breakout = 'time'
                break
                
            if low<lower_border:
                close_price = lower_border
                breakout = 'lower'
                break
            if high>high_border:
                close_price = high_border
                breakout = 'upper'
                break

            it+=1
            

        if breakout=='time':
            if (close_price < middle_2 and mode == 'put') or (close_price > middle_2 and mode == 'call'):
                breakout = 'time_middle_2'
                if mode == 'put':
                    pnl = -abs((((open_price-close_price)*qty)*0.80))
                else:
                    pnl = -abs((((close_price-open_price)*qty)*0.80))
            elif close_price >= open_price:
                breakout = 'time_up'
                if mode == 'put':
                    pnl = ((close_price-open_price)*qty)-ask_cost
                else:
                    pnl = -abs((((close_price-open_price)*qty)+ask_cost))
            elif close_price < open_price:
                breakout = 'time_down'
                if mode == 'put':
                    pnl = -abs((((open_price-close_price)*qty)+ask_cost))
                else:
                    pnl = ((open_price-close_price)*qty)-ask_cost

        if breakout =='upper':
            pnl = up_pnl
        if breakout == 'lower':
            pnl = down_pnl
        
        
        pnl_fut = (close_price-open_price)*qty if mode == 'put' else (open_price-close_price)*qty
        pnl_opt = 0
        if mode == 'put':
            if close_price>strike:
                pnl_opt = -ask_cost
            else:
                pnl_opt = (strike-close_price)*0.01
        else:
            if close_price<strike:
                pnl_opt = -ask_cost
            else:
                pnl_opt = (close_price-strike)*0.01            
        pnl = pnl_opt+pnl_fut
        result = {
            "start_timestamp_ms": start,
            "breakout": breakout,
            "period": duration,
            "all_fees": all_fees,
            "pos_values_pnl": pnl,
            "total_pnl": all_fees+pnl
        }
        sv.sum +=pnl
        
        return result, it
    except Exception as e:
        print(f"❌ Ошибка в simulation (i={i}): {type(e).__name__}: {e}")
        traceback.print_exc()
